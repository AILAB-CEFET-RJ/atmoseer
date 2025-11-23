import argparse
import os
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from threading import Lock
from typing import Iterable, Iterator, Sequence
from xml.etree import ElementTree as ET

import pandas as pd
import requests

REQUEST_INTERVAL_SECONDS = 0.1
MAX_FETCH_ATTEMPTS = 3
BACKOFF_SECONDS = 5
DEFAULT_RATE_PER_SECOND = 2.5
DEFAULT_MAX_WORKERS = 8
REQUEST_TIMEOUT = 30
# The WebSirene API appears to interpret the date as MM/DD/YYYY despite accepting DD/MM/YYYY.
# We try both formats to handle ambiguous days (1-12).
PRIMARY_TIMESTAMP_FORMAT = "%m/%d/%Y %H:%M"
FALLBACK_TIMESTAMP_FORMAT = "%d/%m/%Y %H:%M"


def parse_cli_datetime(raw_datetime: str) -> datetime:
    supported_formats = ("%d/%m/%Y %H:%M", "%d-%m-%Y %H:%M")
    for fmt in supported_formats:
        try:
            return datetime.strptime(raw_datetime, fmt)
        except ValueError:
            continue
    raise ValueError(
        f"'{raw_datetime}' is not in a supported format (expected DD/MM/YYYY HH:MM or DD-MM-YYYY HH:MM)."
    )


def make_time_blocks(start: pd.Timestamp, end: pd.Timestamp, block_days: int) -> Iterator[tuple[pd.Timestamp, pd.Timestamp]]:
    cursor = start
    delta = pd.Timedelta(days=block_days)
    while cursor < end:
        block_end = min(cursor + delta, end)
        yield cursor, block_end
        cursor = block_end


def _clean_text(value: str | None) -> str:
    if value is None:
        return ""
    return value.strip()


def _parse_numeric(value: str | None) -> float | None:
    if value is None:
        return None
    cleaned = value.strip()
    if not cleaned or cleaned.lower() == "null":
        return None
    cleaned = cleaned.replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return None


def _extract_rain_value(chuvas_elem: ET.Element) -> float | None:
    """Prefer chuvas@m5; fallback to other possible numeric fields."""
    if chuvas_elem is None:
        return None
    primary = _parse_numeric(chuvas_elem.get("m5"))
    if primary is not None:
        return primary
    fallback_sources = [
        chuvas_elem.get("chuva"),
        chuvas_elem.get("h01"),
        chuvas_elem.get("h04"),
        chuvas_elem.get("h24"),
        chuvas_elem.get("h96"),
    ]
    for candidate in chuvas_elem.findall(".//chuva"):
        fallback_sources.append(_clean_text(candidate.text))
    for source in fallback_sources:
        parsed = _parse_numeric(source)
        if parsed is not None:
            return parsed
    return None


def _extract_rain_fields(chuvas_elem: ET.Element) -> dict:
    """Return a dict with all rain accumulation windows present in the chuvas element."""
    fields = {
        "m5": _parse_numeric(chuvas_elem.get("m5")) if chuvas_elem is not None else None,
        "m15": _parse_numeric(chuvas_elem.get("m15")) if chuvas_elem is not None else None,
        "h01": _parse_numeric(chuvas_elem.get("h01")) if chuvas_elem is not None else None,
        "h04": _parse_numeric(chuvas_elem.get("h04")) if chuvas_elem is not None else None,
        "h24": _parse_numeric(chuvas_elem.get("h24")) if chuvas_elem is not None else None,
        "h96": _parse_numeric(chuvas_elem.get("h96")) if chuvas_elem is not None else None,
        "mes": _parse_numeric(chuvas_elem.get("mes")) if chuvas_elem is not None else None,
    }
    # Keep backward compatibility: "rains" mirrors m5 when available, otherwise fallback.
    fields["rains"] = fields.get("m5") if fields.get("m5") is not None else _extract_rain_value(chuvas_elem)
    return fields


def _format_duration(seconds: float) -> str:
    seconds = int(seconds)
    mins, sec = divmod(seconds, 60)
    hrs, mins = divmod(mins, 60)
    return f"{hrs:02d}:{mins:02d}:{sec:02d}"


def parse_month_list(raw: str | None) -> set[int]:
    if not raw:
        return set()
    months: set[int] = set()
    for part in raw.split(','):
        part = part.strip()
        if not part:
            continue
        try:
            month = int(part)
        except ValueError:
            raise ValueError(f"Invalid month '{part}'. Use numbers 1-12 separated by commas.")
        if month < 1 or month > 12:
            raise ValueError(f"Invalid month '{part}'. Use numbers 1-12.")
        months.add(month)
    return months


def _extract_station_catalog(xml_content: str) -> list[dict]:
    root = ET.fromstring(xml_content)
    stations: list[dict] = []
    for station_elem in root.findall(".//estacao"):
        raw_id = station_elem.get("id")
        try:
            station_id = int(raw_id) if raw_id is not None else None
        except ValueError:
            station_id = None
        loc_elem = station_elem.find("localizacao")
        latitude = _parse_numeric(loc_elem.get("latitude")) if loc_elem is not None else None
        longitude = _parse_numeric(loc_elem.get("longitude")) if loc_elem is not None else None
        stations.append(
            {
                "id": station_id,
                "nome": _clean_text(station_elem.get("nome")),
                "type": _clean_text(station_elem.get("type")),
                "latitude": latitude,
                "longitude": longitude,
            }
        )
    return [s for s in stations if s.get("id") is not None]


def fetch_station_catalog() -> list[dict]:
    url = "http://websirene.rio.rj.gov.br/xml/chuvas.xml"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    encoding = response.encoding or response.apparent_encoding or "latin-1"
    response.encoding = encoding
    return _extract_station_catalog(response.text)


def _extract_all_station_payloads(xml_content: str, station_ids_filter: set[int]) -> list[dict]:
    root = ET.fromstring(xml_content)
    payloads: list[dict] = []
    for station_elem in root.findall(".//estacao"):
        raw_id = station_elem.get("id")
        if raw_id is None:
            continue
        try:
            current_id = int(raw_id)
        except ValueError:
            continue
        if station_ids_filter and current_id not in station_ids_filter:
            continue

        chuvas_elem = station_elem.find("chuvas")
        observation_dt = None
        rain_value = None
        if chuvas_elem is not None:
            hora_attr = chuvas_elem.get("hora")
            if hora_attr:
                observation_dt = pd.to_datetime(hora_attr, utc=True, errors="coerce")
            rain_value = _extract_rain_value(chuvas_elem)
        rain_fields = _extract_rain_fields(chuvas_elem) if chuvas_elem is not None else {k: None for k in ["m5","m15","h01","h04","h24","h96","mes","rains"]}

        loc_elem = station_elem.find("localizacao")
        latitude = _parse_numeric(loc_elem.get("latitude")) if loc_elem is not None else None
        longitude = _parse_numeric(loc_elem.get("longitude")) if loc_elem is not None else None

        payloads.append(
            {
                "id": current_id,
                "nome": _clean_text(station_elem.get("nome")),
                "type": _clean_text(station_elem.get("type")),
                "latitude": latitude,
                "longitude": longitude,
                "rains": rain_value,
                "m5": rain_fields.get("m5"),
                "m15": rain_fields.get("m15"),
                "h01": rain_fields.get("h01"),
                "h04": rain_fields.get("h04"),
                "h24": rain_fields.get("h24"),
                "h96": rain_fields.get("h96"),
                "mes": rain_fields.get("mes"),
                "observation_datetime": observation_dt,
            }
        )
    return payloads


@dataclass
class RateLimiter:
    rate_per_second: float
    allowance: float = 0.0
    last_check: float = time.monotonic()
    _lock: Lock = field(default_factory=Lock, init=False, repr=False)

    def acquire(self) -> None:
        if self.rate_per_second <= 0:
            return
        with self._lock:
            current = time.monotonic()
            time_passed = current - self.last_check
            self.last_check = current
            self.allowance += time_passed * self.rate_per_second
            if self.allowance > self.rate_per_second:
                self.allowance = self.rate_per_second
            if self.allowance < 1.0:
                sleep_time = (1.0 - self.allowance) / self.rate_per_second
                time.sleep(sleep_time)
                self.allowance = 0.0
            else:
                self.allowance -= 1.0


def _fetch_with_format(ts: pd.Timestamp, ts_format: str, station_ids_filter: set[int], session: requests.Session, rate_limiter: RateLimiter) -> list[dict]:
    rate_limiter.acquire()
    timestamp_str = ts.strftime(ts_format)
    url = f"http://websirene.rio.rj.gov.br/xml/chuvas.xml?time={timestamp_str}"
    for attempt in range(1, MAX_FETCH_ATTEMPTS + 1):
        try:
            response = session.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            encoding = response.encoding or response.apparent_encoding or "latin-1"
            response.encoding = encoding
            payloads = _extract_all_station_payloads(response.text, station_ids_filter)
            if not payloads:
                return []
            df = pd.DataFrame(payloads)
            df["request_datetime"] = ts
            df["observation_datetime"] = pd.to_datetime(df["observation_datetime"], utc=True, errors="coerce")
            # Keep only rows matching the requested calendar date to avoid swapped month/day.
            df = df[df["observation_datetime"].dt.date == ts.date()]
            if df.empty:
                return []
            return df.to_dict(orient="records")
        except (requests.exceptions.RequestException, ET.ParseError):
            if attempt < MAX_FETCH_ATTEMPTS:
                time.sleep(BACKOFF_SECONDS * attempt)
            continue
    return []


def fetch_timestamp(ts: pd.Timestamp, station_ids_filter: set[int], session: requests.Session, rate_limiter: RateLimiter) -> list[dict]:
    # Try primary (MM/DD) first, fallback to DD/MM if observation months clearly mismatch.
    recs_primary = _fetch_with_format(ts, PRIMARY_TIMESTAMP_FORMAT, station_ids_filter, session, rate_limiter)
    if recs_primary:
        obs_months = pd.DataFrame(recs_primary)["observation_datetime"].dt.month.dropna()
        if not obs_months.empty and obs_months.mode().iloc[0] == ts.month:
            return recs_primary
    recs_fallback = _fetch_with_format(ts, FALLBACK_TIMESTAMP_FORMAT, station_ids_filter, session, rate_limiter)
    return recs_fallback or recs_primary


def group_and_dedup(records: Iterable[dict]) -> dict[int, pd.DataFrame]:
    per_station: dict[int, list[dict]] = defaultdict(list)
    for rec in records:
        if rec is None:
            continue
        station_id = rec.get("id")
        if station_id is None:
            continue
        per_station[int(station_id)].append(rec)
    result: dict[int, pd.DataFrame] = {}
    for station_id, recs in per_station.items():
        df = pd.DataFrame(recs)
        if df.empty:
            continue
        df = df.dropna(subset=["observation_datetime"])
        if df.empty:
            continue
        df = df.drop_duplicates(subset=["observation_datetime"])
        df = df.sort_values(by="observation_datetime")
        result[station_id] = df
    return result


def write_partitioned(df: pd.DataFrame, output_dir: str, station_id: int, fmt: str = "parquet") -> None:
    if df.empty:
        return
    df = df.copy()
    df["year"] = df["observation_datetime"].dt.year
    for year, chunk in df.groupby("year"):
        base_dir = os.path.join(output_dir, f"station_id={station_id}", f"year={int(year)}")
        os.makedirs(base_dir, exist_ok=True)
        if fmt == "parquet":
            path = os.path.join(base_dir, "data.parquet")
            try:
                if os.path.exists(path):
                    existing = pd.read_parquet(path)
                    chunk = pd.concat([existing, chunk], ignore_index=True)
                chunk = chunk.drop_duplicates(subset=["observation_datetime"])
                chunk.sort_values(by="observation_datetime", inplace=True)
                chunk.to_parquet(path, index=False)
                continue
            except Exception:
                # fallback to CSV on parquet errors
                fmt = "csv"
        path = os.path.join(base_dir, "data.csv")
        if os.path.exists(path):
            existing = pd.read_csv(path, parse_dates=["observation_datetime", "request_datetime"])
            chunk = pd.concat([existing, chunk], ignore_index=True)
        chunk = chunk.drop_duplicates(subset=["observation_datetime"])
        chunk.sort_values(by="observation_datetime", inplace=True)
        chunk.to_csv(path, index=False)


def load_index(index_path: str) -> pd.DataFrame:
    if not os.path.exists(index_path):
        return pd.DataFrame(columns=["station_id", "first_obs", "last_obs", "last_request_dt", "row_count"])
    return pd.read_csv(index_path, parse_dates=["first_obs", "last_obs", "last_request_dt"])


def update_index(index_df: pd.DataFrame, station_id: int, df: pd.DataFrame, request_dt: pd.Timestamp) -> pd.DataFrame:
    if df.empty:
        return index_df
    first_obs = df["observation_datetime"].min()
    last_obs = df["observation_datetime"].max()
    row_count = len(df)
    existing = index_df[index_df["station_id"] == station_id]
    if existing.empty:
        new_row = pd.DataFrame(
            {
                "station_id": [station_id],
                "first_obs": [first_obs],
                "last_obs": [last_obs],
                "last_request_dt": [request_dt],
                "row_count": [row_count],
            }
        )
        # Avoid FutureWarning when concatenating with an empty/all-NA frame.
        return new_row if index_df.empty else pd.concat([index_df, new_row], ignore_index=True)
    idx = existing.index[0]
    index_df.loc[idx, "first_obs"] = min(first_obs, existing.iloc[0]["first_obs"])
    index_df.loc[idx, "last_obs"] = max(last_obs, existing.iloc[0]["last_obs"])
    index_df.loc[idx, "last_request_dt"] = request_dt
    index_df.loc[idx, "row_count"] = existing.iloc[0]["row_count"] + row_count
    return index_df


def should_skip(index_df: pd.DataFrame, station_id: int, ts: pd.Timestamp) -> bool:
    row = index_df[index_df["station_id"] == station_id]
    if row.empty:
        return False
    last_obs = row.iloc[0]["last_obs"]
    if pd.isna(last_obs):
        return False
    return ts <= last_obs


def collect_block(
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    station_ids: Sequence[int],
    rate_limiter: RateLimiter,
    args,
    index_df: pd.DataFrame,
) -> tuple[list[dict], pd.Timestamp]:
    records: list[dict] = []
    session = requests.Session()
    station_filter_set = set(station_ids)
    timestamps = pd.date_range(start=start_ts, end=end_ts, freq="10min", inclusive="both")
    with ThreadPoolExecutor(max_workers=args.max_workers) as pool:
        futures = []
        for ts in timestamps:
            if args.ignore_months and ts.month in args.ignore_months:
                continue
            if args.resume:
                # If all stations already beyond last_obs, skip the timestamp entirely.
                if all(should_skip(index_df, station_id, ts) for station_id in station_ids):
                    continue
            futures.append(pool.submit(fetch_timestamp, ts, station_filter_set, session, rate_limiter))

        for future in as_completed(futures):
            recs = future.result()
            if not recs:
                continue
            for rec in recs:
                obs_dt = rec.get("observation_datetime")
                if pd.isna(obs_dt):
                    continue
                if obs_dt < start_ts or obs_dt > end_ts:
                    continue
                records.append(rec)
    last_request_dt = end_ts
    return records, last_request_dt


def run_pipeline(args):
    overall_start = time.monotonic()
    try:
        start_dt = parse_cli_datetime(args.start_date)
        end_dt = parse_cli_datetime(args.end_date)
    except ValueError as err:
        print(f"Invalid datetime: {err}")
        sys.exit(1)

    if end_dt < start_dt:
        print("End date/time must be greater than or equal to the start date/time.")
        sys.exit(1)

    start_dt_utc = pd.to_datetime(start_dt).tz_localize("UTC")
    end_dt_utc = pd.to_datetime(end_dt).tz_localize("UTC")

    try:
        ignore_months = parse_month_list(args.ignore_months)
    except ValueError as err:
        print(f"Invalid --ignore-months: {err}")
        sys.exit(1)
    args.ignore_months = ignore_months

    print(f"Period: {start_dt.strftime('%d/%m/%Y %H:%M')} to {end_dt.strftime('%d/%m/%Y %H:%M')}")

    # Catalog
    if args.stations == "all":
        try:
            catalog = fetch_station_catalog()
            station_ids = sorted(int(s["id"]) for s in catalog)
        except Exception as e:
            print(f"Failed to load station catalog: {e}")
            sys.exit(1)
    else:
        station_ids = sorted(int(s) for s in args.stations.split(",") if s.strip())

    os.makedirs(args.output_dir, exist_ok=True)
    index_path = os.path.join(args.output_dir, "index.csv")
    index_df = load_index(index_path) if args.resume else pd.DataFrame(columns=["station_id", "first_obs", "last_obs", "last_request_dt", "row_count"])

    rate_limiter = RateLimiter(rate_per_second=args.rate_per_second)

    for block_start, block_end in make_time_blocks(start_dt_utc, end_dt_utc, args.block_size_days):
        print(f"Processing block {block_start} -> {block_end} ({len(station_ids)} stations)")
        records, last_request_dt = collect_block(block_start, block_end, station_ids, rate_limiter, args, index_df)
        grouped = group_and_dedup(records)
        if not grouped:
            continue
        for station_id, df in grouped.items():
            write_partitioned(df, args.output_dir, station_id, fmt=args.format)
            index_df = update_index(index_df, station_id, df, last_request_dt)
        index_df.to_csv(index_path, index=False)

    elapsed = time.monotonic() - overall_start
    print(f"Done in {_format_duration(elapsed)}. Index saved at {index_path}")


def build_arg_parser():
    parser = argparse.ArgumentParser(description="Download full historical rain data for WebSirene stations.")
    parser.add_argument("--start-date", required=True, help="Start date/time (DD/MM/YYYY HH:MM)")
    parser.add_argument("--end-date", required=True, help="End date/time (DD/MM/YYYY HH:MM)")
    parser.add_argument("--stations", default="all", help="Comma-separated station IDs or 'all'")
    parser.add_argument("--output-dir", default="data/ws/full", help="Output directory for partitioned data")
    parser.add_argument("--max-workers", type=int, default=DEFAULT_MAX_WORKERS, help="Concurrent workers (threads)")
    parser.add_argument("--rate-per-second", type=float, default=DEFAULT_RATE_PER_SECOND, help="Max requests per second")
    parser.add_argument("--block-size-days", type=int, default=1, help="Block size in days for processing")
    parser.add_argument("--format", choices=["parquet", "csv"], default="parquet", help="Output format")
    parser.add_argument("--resume", action="store_true", help="Resume using existing index to skip completed timestamps")
    parser.add_argument("--ignore-months", default=None, help="Comma-separated list of months (1-12) to skip entirely, e.g., '6,7,8'")
    return parser


if __name__ == "__main__":
    args = build_arg_parser().parse_args()
    run_pipeline(args)
