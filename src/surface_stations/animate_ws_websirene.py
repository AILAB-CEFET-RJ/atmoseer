"""
Create an animated Folium map for WebSirene precipitation.

Usage example:
PYTHONPATH=src python3 src/surface_stations/animate_ws_websirene.py \
  --start-date "01/01/2013 00:00" \
  --end-date "03/01/2013 00:00" \
  --bbox "-23.0,-44.5,-22.0,-42.5" \
  --accum-field m15 \
  --output-html websirene_animation.html
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import Iterable

import folium
import pandas as pd
from branca.colormap import LinearColormap, linear
from folium.plugins import TimestampedGeoJson

# Mapping from accumulation field to pandas frequency and ISO8601 period
FIELD_FREQ = {
    "m5": ("5min", "PT5M"),
    "m15": ("15min", "PT15M"),
    "h01": ("1h", "PT1H"),
    "h04": ("4h", "PT4H"),
    "h24": ("24h", "PT24H"),
    "h96": ("96h", "PT96H"),
}
# Window length in minutes for each accumulation field (used to derive mm/h intensity)
FIELD_MINUTES = {
    "m5": 5,
    "m15": 15,
    "h01": 60,
    "h04": 240,
    "h24": 1440,
    "h96": 5760,
}

# Rain rate categories in mm/h
PRECIPITATION_LEVELS = [
    (0, 5, "Chuva fraca / sem chuva", "#c7e9ff"),
    (5, 25, "Chuva moderada", "#fdd49e"),
    (25, 50, "Chuva forte", "#f46d43"),
    (50, float("inf"), "Chuva extrema", "#b2172b"),
]


def add_precipitation_legend(fmap: folium.Map) -> None:
    rows = []
    for lower, upper, label, color in PRECIPITATION_LEVELS:
        upper_str = "∞" if upper == float("inf") else f"{upper:g}"
        rows.append(
            f'<div style="display:flex;align-items:center;margin-bottom:4px;">'
            f'<span style="display:inline-block;width:14px;height:14px;background:{color};margin-right:6px;border:1px solid #555"></span>'
            f'<span style="font-size:12px;">{label} ({lower:g}–{upper_str} mm/h)</span>'
            f"</div>"
        )
    legend_html = (
        '<div style="position: fixed; bottom: 80px; left: 12px; z-index:9999; '
        "background: white; padding: 8px 10px; border: 1px solid #666; "
        'box-shadow: 0 1px 4px rgba(0,0,0,0.3); font-family: sans-serif;">'
        '<div style="font-weight:600; margin-bottom:6px;">Níveis de precipitação (mm/h)</div>'
        + "".join(rows)
        + "</div>"
    )
    fmap.get_root().html.add_child(folium.Element(legend_html))


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


def parse_bbox(raw_bbox: list[str]) -> tuple[float, float, float, float]:
    # Supports either one token with commas or four separate tokens.
    if len(raw_bbox) == 1:
        parts = raw_bbox[0].split(",")
    else:
        parts = raw_bbox
    try:
        floats = [float(x.strip()) for x in parts]
    except ValueError:
        raise ValueError("Bounding box must be four numbers: lat_min,lon_min,lat_max,lon_max")
    if len(floats) != 4:
        raise ValueError("Bounding box must have four values: lat_min,lon_min,lat_max,lon_max")
    lat_min, lon_min, lat_max, lon_max = floats
    if lat_min >= lat_max or lon_min >= lon_max:
        raise ValueError("Invalid bbox: ensure lat_min < lat_max and lon_min < lon_max")
    return lat_min, lon_min, lat_max, lon_max


def _read_station_file(path: Path, field: str) -> pd.DataFrame:
    usecols = ["id", "nome", "latitude", "longitude", "observation_datetime", field]
    parse_dates = ["observation_datetime"]
    if path.suffix == ".parquet":
        return pd.read_parquet(path, columns=usecols)
    return pd.read_csv(path, usecols=usecols, parse_dates=parse_dates)


def _iter_data_files(base_dir: Path, years: Iterable[int]) -> Iterable[Path]:
    for station_dir in base_dir.glob("station_id=*"):
        for year in years:
            parquet_path = station_dir / f"year={year}" / "data.parquet"
            csv_path = station_dir / f"year={year}" / "data.csv"
            if parquet_path.exists():
                yield parquet_path
            elif csv_path.exists():
                yield csv_path


def load_filtered_data(
    data_dir: Path,
    start_dt: pd.Timestamp,
    end_dt: pd.Timestamp,
    bbox: tuple[float, float, float, float],
    field: str,
) -> pd.DataFrame:
    lat_min, lon_min, lat_max, lon_max = bbox
    years = range(start_dt.year, end_dt.year + 1)
    frames = []
    for path in _iter_data_files(data_dir, years):
        try:
            df = _read_station_file(path, field)
        except Exception:
            continue
        if df.empty or field not in df.columns:
            continue
        df["observation_datetime"] = pd.to_datetime(df["observation_datetime"], utc=True, errors="coerce")
        df = df.dropna(subset=["latitude", "longitude", field, "observation_datetime"])
        df = df[
            (df["latitude"] >= lat_min)
            & (df["latitude"] <= lat_max)
            & (df["longitude"] >= lon_min)
            & (df["longitude"] <= lon_max)
            & (df["observation_datetime"] >= start_dt)
            & (df["observation_datetime"] <= end_dt)
        ]
        if df.empty:
            continue
        frames.append(df)
    if not frames:
        return pd.DataFrame(columns=["id", "nome", "latitude", "longitude", "observation_datetime", field])
    return pd.concat(frames, ignore_index=True)


def build_features(df: pd.DataFrame, field: str, colormap, freq: str) -> list[dict]:
    df = df.copy()
    df["frame_time"] = pd.to_datetime(df["observation_datetime"], utc=True).dt.floor(freq)
    minutes = FIELD_MINUTES.get(field, 60)
    df["rate_mm_h"] = df[field] * 60.0 / minutes
    df.sort_values(by="frame_time", inplace=True)
    features: list[dict] = []
    for _, row in df.iterrows():
        value = float(row[field])
        rate = float(row["rate_mm_h"])
        color = colormap(rate)
        popup = (
            f"<b>{row.get('nome', 'Estacao')}</b><br>"
            f"{row['frame_time']}<br>"
            f"{field}: {value:.2f} mm<br>"
            f"Intensidade: {rate:.2f} mm/h"
        )
        naive_time = row["frame_time"].tz_convert(None)
        iso_time = naive_time.isoformat()
        hex_color = color if isinstance(color, str) else "#000000"
        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [float(row["longitude"]), float(row["latitude"])],
                },
                "properties": {
                    "times": [iso_time],
                    "popup": popup,
                    "style": {"color": hex_color, "fillColor": hex_color},
                    "icon": "circle",
                    "iconstyle": {
                        "fillColor": hex_color,
                        "fillOpacity": 0.85,
                        "stroke": True,
                        "color": hex_color,
                        "radius": 7,
                    },
                },
            }
        )
    return features


def create_map(
    features: list[dict],
    bbox: tuple[float, float, float, float],
    colormap,
    stations_df: pd.DataFrame,
    period: str,
    transition_ms: int,
    duration_ms: int,
    tiles: str,
    zoom_start: int,
) -> folium.Map:
    lat_min, lon_min, lat_max, lon_max = bbox
    center_lat = (lat_min + lat_max) / 2
    center_lon = (lon_min + lon_max) / 2
    fmap = folium.Map(location=[center_lat, center_lon], zoom_start=zoom_start, tiles=tiles)
    duration_js = "undefined"
    if duration_ms is not None and duration_ms > 0:
        seconds = max(1, round(duration_ms / 1000))
        duration_js = f"\"PT{seconds}S\""
    layer = TimestampedGeoJson(
        {"type": "FeatureCollection", "features": features},
        period=period,
        transition_time=transition_ms,
        duration=None,  # overridden below
        add_last_point=False,
        auto_play=True,
        loop=True,
        loop_button=True,
        time_slider_drag_update=True,
    )
    layer.duration = duration_js
    layer.add_to(fmap)
    # Static location markers for station positions (small, neutral color).
    if not stations_df.empty:
        loc_group = folium.FeatureGroup(name="Estações (referência)", overlay=True, control=False)
        for _, row in stations_df.iterrows():
            folium.CircleMarker(
                location=[float(row["latitude"]), float(row["longitude"])],
                radius=2,
                color="#555",
                weight=1,
                fill=True,
                fill_color="#cccccc",
                fill_opacity=0.7,
                opacity=0.8,
                tooltip=row.get("nome", f"Estação {row.get('id','')}"),
            ).add_to(loc_group)
        loc_group.add_to(fmap)
    folium.Rectangle(bounds=[[lat_min, lon_min], [lat_max, lon_max]], color="#2c7bb6", fill=False).add_to(fmap)
    colormap.add_to(fmap)
    add_precipitation_legend(fmap)
    return fmap


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Animate WebSirene precipitation using Folium TimestampedGeoJson."
    )
    parser.add_argument("--start-date", required=True, help="Start date/time (DD/MM/YYYY HH:MM)")
    parser.add_argument("--end-date", required=True, help="End date/time (DD/MM/YYYY HH:MM)")
    parser.add_argument(
        "--bbox",
        required=True,
        nargs="+",
        help="Bounding box as lat_min,lon_min,lat_max,lon_max (e.g., '-23.0,-44.5,-22.0,-42.5' or '-23.0 -44.5 -22.0 -42.5')",
    )
    parser.add_argument(
        "--accum-field",
        choices=list(FIELD_FREQ.keys()),
        default="m5",
        help="Accumulation field to visualise (controls frame interval).",
    )
    parser.add_argument("--data-dir", default="data/ws/full", help="Directory containing partitioned WebSirene data.")
    parser.add_argument("--output-html", default="websirene_animation.html", help="Output HTML path for the map.")
    parser.add_argument(
        "--transition-ms",
        type=int,
        default=300,
        help="Animation transition time between frames (milliseconds).",
    )
    parser.add_argument(
        "--frame-duration-ms",
        type=int,
        default=800,
        help="How long each frame persists (milliseconds).",
    )
    parser.add_argument(
        "--hide-zeros",
        action="store_true",
        help="Hide observations with zero (or negative) accumulation.",
    )
    parser.add_argument("--zoom-start", type=int, default=10, help="Initial zoom level for the map.")
    parser.add_argument("--tiles", default="cartodbpositron", help="Folium tileset.")
    parser.add_argument(
        "--max-scale",
        type=float,
        default=None,
        help="Optional fixed upper bound for the color scale; defaults to dataset max.",
    )
    return parser


def main():
    args = build_arg_parser().parse_args()
    try:
        start_dt = parse_cli_datetime(args.start_date)
        end_dt = parse_cli_datetime(args.end_date)
    except ValueError as err:
        print(f"Invalid datetime: {err}")
        sys.exit(1)
    if end_dt < start_dt:
        print("End date/time must be greater than or equal to the start date/time.")
        sys.exit(1)
    try:
        bbox = parse_bbox(args.bbox)
    except ValueError as err:
        print(f"Invalid bbox: {err}")
        sys.exit(1)

    freq, period = FIELD_FREQ[args.accum_field]
    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        print(f"Data directory not found: {data_dir}")
        sys.exit(1)

    start_ts = pd.to_datetime(start_dt, utc=True)
    end_ts = pd.to_datetime(end_dt, utc=True)
    df = load_filtered_data(data_dir, start_ts, end_ts, bbox, args.accum_field)
    stations_df = df[["id", "nome", "latitude", "longitude"]].drop_duplicates()
    if args.hide_zeros:
        df = df[df[args.accum_field] > 0]
    if df.empty:
        print("No data found for the given filters (dates, bbox, accumulation field).")
        sys.exit(1)

    # Build a discrete colormap based on AlertaRio thresholds (rates in mm/h).
    def alertario_color(rate: float) -> str:
        for lower, upper, _, hex_color in PRECIPITATION_LEVELS:
            if lower <= rate < upper:
                return hex_color
        return PRECIPITATION_LEVELS[-1][3]

    # For legend rendering we also keep a discrete colormap for the caption.
    # Discrete colormap for legend; last bin covers 50+ mm/h.
    colormap = LinearColormap(
        colors=[lvl[3] for lvl in PRECIPITATION_LEVELS],
        vmin=0,
        vmax=50,
        index=[0, 5, 25, 50],
    )
    colormap.caption = "Intensidade (mm/h) - níveis AlertaRio"

    features = build_features(df, args.accum_field, alertario_color, freq)
    if not features:
        print("No features to render after filtering. Check date range, bbox, and accumulation field.")
        sys.exit(1)
    fmap = create_map(
        features=features,
        bbox=bbox,
        colormap=colormap,
        stations_df=stations_df,
        period=period,
        transition_ms=args.transition_ms,
        duration_ms=args.frame_duration_ms,
        tiles=args.tiles,
        zoom_start=args.zoom_start,
    )
    output_path = Path(args.output_html)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fmap.save(output_path)
    print(f"Animation saved to {output_path}")


if __name__ == "__main__":
    main()
