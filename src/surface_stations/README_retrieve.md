# Surface Station Data Retrieval

This directory concentrates the scripts that download ("retrieve") raw observations from the different weather-station systems supported by AtmoSeer. Use this document as a quick reference for the available collectors, their required credentials and typical command lines.

## Before You Start
- Execute the commands from the project root with the Conda environment activated (e.g. `conda activate atmoseer`).
- Most scripts expect `PYTHONPATH=src`. The examples below set this explicitly; alternatively, use the Make targets that already export it for you.
- Sensitive credentials (API tokens, email/password) are passed through the command line. Prefer exporting them as environment variables and referencing those variables in the examples.

## Summary of Retrieval Scripts

| Script | System | Output | Notes |
| --- | --- | --- | --- |
| `retrieve_ws_inmet.py` | INMET automatic stations | `data/ws/inmet/<ID>.parquet` | Requires an INMET API token; pulls one or more full years per request. |
| `retrieve_ws_cemaden.py` | Cemaden PCD network | `data/ws/cemaden/raw/<CODE>.parquet` | Requires credentials; handles token caching (`config/token_demaden.json`). |
| `retrieve_ws_alertario.py` | AlertaRio (COR/RJ) meteorological stations | `data/landing/<station>.csv` | Works on previously downloaded text files under `data/RAW_data/COR/meteorologica`. |
| `retrieve_ws_redemet.py` | REDEMET aerodrome reports | `<output>.csv` + `log.csv` | Requires a DECEA API key; runs hourly requests for the selected time span. |

Each section below details CLI options, expected inputs and a runnable example.

## INMET - `retrieve_ws_inmet.py`
Downloads hourly observations from the INMET API for one of the stations listed in `config/globals.py`.

**Requirements**
- INMET API token (`-t/--api_token`).
- Station id (`-s/--station_id`) present in `globals.INMET_WEATHER_STATION_IDS`.
- Begin and end years (`-b/--begin_year`, `-e/--end_year`). The script automatically clamps the end date to "today" to avoid future requests.

**Usage**
```bash
PYTHONPATH=src python3 src/surface_stations/retrieve_ws_inmet.py \
  --api_token "$INMET_TOKEN" \
  --station_id A601 \
  --begin_year 2019 \
  --end_year 2023
```
The resulting Parquet file is written to `data/ws/inmet/A601.parquet`.

**Make shortcut**
```bash
make inmet_retrieve_ws API_TOKEN="$INMET_TOKEN" STATION_ID=A601 BEGIN_YEAR=2019 END_YEAR=2023
```

## Cemaden - `retrieve_ws_cemaden.py`
Calls the Cemaden REST API to fetch data either by municipality (IBGE code) or by explicit station code. The script manages authentication tokens and persists them in `config/token_demaden.json` to reduce login calls.

**Requirements**
- Cemaden API credentials (`--email`, `--senha`).
- Either a list of IBGE municipality codes (`--ibge 3304557 3301702`) or a specific station (`--estacao <code>`).
- Optional time bounds (`--inicio YYYY-MM-DD`, `--fim YYYY-MM-DD`). Defaults span from 2015-01-01 to "today".

**Usage**
```bash
PYTHONPATH=src python3 src/surface_stations/retrieve_ws_cemaden.py \
  --ibge 3304557 \
  --inicio 2023-01-01 \
  --fim 2023-12-31 \
  --email "$CEMADEN_EMAIL" \
  --senha "$CEMADEN_PASSWORD"
```
When multiple municipalities or stations are requested, one Parquet file per station is created under `data/ws/cemaden/raw/`.

## AlertaRio - `retrieve_ws_alertario.py`
Processes the monthly text files provided by COR/AlertaRio for the Rio de Janeiro stations. It standardises the fixed-width format, cleans sentinel values and exports hourly CSVs.

**Requirements**
- Local files organised at `data/RAW_data/COR/meteorologica/` following the naming convention `<station>_<YYYY><MM>_Met.txt`.
- Station name via `-s/--station` (use `all` to process the predefined list in `STATION_NAMES_FOR_RJ`).
- Optional year bounds (`-b/--begin`, `-e/--end`). Defaults cover 1997 up to the current year.

**Usage**
```bash
PYTHONPATH=src python3 src/surface_stations/retrieve_ws_alertario.py \
  --station guaratiba \
  --begin 2018 \
  --end 2024
```
Outputs are CSV files in `data/landing/<station>.csv`. Intermediate corrected files are stored under `data/RAW_data/COR/meteorologica/aux_<station>/`.

## REDEMET - `retrieve_ws_redemet.py`
Retrieves METAR-like observations from the DECEA/REDEMET API for aerodrome stations (e.g. SBGL, SBRJ). The script walks hour-by-hour across the requested time span, logging the status of each request.

**Requirements**
- Valid REDEMET API key (`-k/--key`).
- ICAO station code (`-s/--station`).
- Start and end datetimes formatted as `AAAAMMDDHH` (`-start/--start_date`, `-end/--end_date`).
- Desired output name (`-o/--output_file`). The script saves `<output_file>.csv` plus a `log.csv` with the HTTP status per hour.

**Usage**
```bash
python3 src/surface_stations/retrieve_ws_redemet.py \
  --key "$REDEMET_KEY" \
  --station SBGL \
  --start_date 2023010100 \
  --end_date 2023010723 \
  --output_file data/ws/redemet/SBGL_202301
```
If the API returns HTTP 429 (rate limit), the script waits and retries automatically. Large ranges may take considerable time because one request is issued per hour.

---
Need to support another station system? Create a new retrieval script beside the ones above and add its documentation to this file so the team can discover it easily.
