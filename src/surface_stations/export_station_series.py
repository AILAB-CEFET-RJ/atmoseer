"""
Export a single WebSirene station time series to CSV or Parquet for a given date range.

Example:
PYTHONPATH=src python3 src/surface_stations/export_station_series.py \
  --station-id 15 \
  --start-date "01/01/2013 00:00" \
  --end-date "03/01/2013 00:00" \
  --format parquet \
  --output data/ws/export/station_15_20130101_20130103.parquet
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd


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


def _iter_station_files(base_dir: Path, station_id: int, years: Iterable[int]) -> list[Path]:
    paths: list[Path] = []
    for year in years:
        parquet_path = base_dir / f"station_id={station_id}" / f"year={year}" / "data.parquet"
        csv_path = base_dir / f"station_id={station_id}" / f"year={year}" / "data.csv"
        if parquet_path.exists():
            paths.append(parquet_path)
        elif csv_path.exists():
            paths.append(csv_path)
    return paths


def _read_file(path: Path) -> pd.DataFrame:
    if path.suffix == ".parquet":
        df = pd.read_parquet(path)
    else:
        df = pd.read_csv(path, parse_dates=["observation_datetime", "request_datetime"], infer_datetime_format=True)
    df["observation_datetime"] = pd.to_datetime(df["observation_datetime"], utc=True, errors="coerce")
    return df


def main():
    parser = argparse.ArgumentParser(
        description="Export a single WebSirene station series to CSV or Parquet for a given date range."
    )
    parser.add_argument("--station-id", required=True, type=int, help="Station identifier (integer).")
    parser.add_argument("--start-date", required=True, help="Start date/time (DD/MM/YYYY HH:MM)")
    parser.add_argument("--end-date", required=True, help="End date/time (DD/MM/YYYY HH:MM)")
    parser.add_argument("--data-dir", default="data/ws/full", help="Base directory containing partitioned WebSirene data.")
    parser.add_argument("--format", choices=["csv", "parquet"], default="parquet", help="Output format.")
    parser.add_argument(
        "--output",
        default=None,
        help="Output file path. Defaults to data/ws/export/station_<id>_<start>_<end>.<ext>",
    )
    args = parser.parse_args()

    try:
        start_dt = parse_cli_datetime(args.start_date)
        end_dt = parse_cli_datetime(args.end_date)
    except ValueError as err:
        print(f"Invalid datetime: {err}")
        sys.exit(1)

    if end_dt < start_dt:
        print("End date/time must be greater than or equal to the start date/time.")
        sys.exit(1)

    start_ts = pd.to_datetime(start_dt, utc=True)
    end_ts = pd.to_datetime(end_dt, utc=True)

    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        print(f"Data directory not found: {data_dir}")
        sys.exit(1)

    years = range(start_ts.year, end_ts.year + 1)
    files = _iter_station_files(data_dir, args.station_id, years)
    if not files:
        print(f"No files found for station {args.station_id} in {data_dir} for years {start_ts.year}-{end_ts.year}.")
        sys.exit(1)

    frames = []
    for path in files:
        try:
            frames.append(_read_file(path))
        except Exception as exc:
            print(f"Skipping {path} due to error: {exc}")
    if not frames:
        print("No data could be loaded from available files.")
        sys.exit(1)

    df = pd.concat(frames, ignore_index=True)
    df = df.dropna(subset=["observation_datetime"])
    df = df[
        (df["observation_datetime"] >= start_ts)
        & (df["observation_datetime"] <= end_ts)
    ].sort_values("observation_datetime")

    if df.empty:
        print("No observations in the requested period for this station.")
        sys.exit(1)

    out_path = Path(args.output) if args.output else None
    if out_path is None:
        out_dir = Path("data/ws/export")
        out_dir.mkdir(parents=True, exist_ok=True)
        start_tag = start_ts.strftime("%Y%m%d%H%M")
        end_tag = end_ts.strftime("%Y%m%d%H%M")
        out_path = out_dir / f"station_{args.station_id}_{start_tag}_{end_tag}.{args.format}"
    else:
        out_path.parent.mkdir(parents=True, exist_ok=True)

    if args.format == "parquet":
        df.to_parquet(out_path, index=False)
    else:
        df.to_csv(out_path, index=False)

    print(f"Saved {len(df)} rows to {out_path}")


if __name__ == "__main__":
    main()
