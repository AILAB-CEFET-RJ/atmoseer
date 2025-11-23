# WebSirene Tools

Scripts to fetch, export, and visualize WebSirene station data.

## 1. Historical fetch (`retrieve_ws_websirene.py`)
Downloads historical data (partitioned by station/year) to Parquet or CSV, with rate limiting, optional resume, and ambiguous-date filtering.

Example:
```bash
PYTHONPATH=src python3 src/surface_stations/retrieve_ws_websirene.py \
  --start-date "01/01/2019 00:00" \
  --end-date "31/12/2019 23:59" \
  --stations all \
  --output-dir data/ws/full \
  --format parquet \
  --rate-per-second 2.5 \
  --block-size-days 3 \
  --ignore-months "6,7,8" \
  --resume
```
Key flags:
- `--start-date/--end-date` (DD/MM/YYYY HH:MM)
- `--stations` (`all` or comma-separated IDs)
- `--output-dir` for partitioned files
- `--format` (`parquet` or `csv`)
- `--rate-per-second`, `--max-workers`, `--block-size-days`, `--resume`
- `--ignore-months` to skip specific months

Note: for ambiguous dates (day ≤ 12), the script tries both formats (MM/DD and DD/MM) and keeps only rows whose `observation_datetime.date()` matches the requested date, avoiding swapped month/day.

## 2. Export a station series (`export_station_series.py`)
Extracts a single station series over a time window and writes Parquet or CSV.

Example:
```bash
PYTHONPATH=src python3 src/surface_stations/export_station_series.py \
  --station-id 15 \
  --start-date "01/01/2019 00:00" \
  --end-date "31/12/2019 23:59" \
  --format parquet \
  --output data/ws/export/station_15_20190101_20191231.parquet
```
Key flags:
- `--station-id` (required)
- `--start-date/--end-date` (DD/MM/YYYY HH:MM)
- `--data-dir` (default `data/ws/full`)
- `--format` (`csv` or `parquet`)
- `--output` (optional; if omitted, writes to `data/ws/export/…`)

## 3. Interactive animation (`animate_ws_websirene.py`)
Builds a Folium map with a temporal animation of rainfall over a bounding box using `TimestampedGeoJson`.

Example:
```bash
PYTHONPATH=src python3 src/surface_stations/animate_ws_websirene.py \
  --start-date "08/04/2019 00:00" \
  --end-date "09/04/2019 00:00" \
  --bbox -23.0 -44.5 -22.0 -42.5 \
  --accum-field m15 \
  --frame-duration-ms 1000 \
  --hide-zeros \
  --output-html websirene_animation.html
```
Key flags:
- `--start-date/--end-date` (DD/MM/YYYY HH:MM)
- `--bbox` (lat_min lon_min lat_max lon_max)
- `--accum-field` (`m5`, `m15`, `h01`, `h04`, `h24`, `h96`)
- `--data-dir` (default `data/ws/full`)
- `--output-html` HTML output path
- `--transition-ms`, `--frame-duration-ms`, `--zoom-start`, `--tiles`
- `--hide-zeros` to omit zero/negative accumulations

Visual details:
- Categorical colormap based on AlertaRio levels (mm/h): light/none (≤5), moderate (>5–25), strong (>25–50), extreme (>50).
- Small neutral markers show all station locations in the ROI; larger colored markers show rainfall per frame.
- Fixed legend in the lower-left corner.
