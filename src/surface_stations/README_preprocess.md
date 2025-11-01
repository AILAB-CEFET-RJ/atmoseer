# Surface Station Preprocessing

This guide explains how to configure and execute `src/surface_stations/preprocess.py`, the script that standardises and engineers features from the raw weather-station datasets.

## 1. Prerequisites
- Execute commands from the project root (`~/ailab/atmoseer`).
- Activate the Conda environment that contains the project dependencies (e.g. `conda activate atmoseer`).
- Ensure `PYTHONPATH` includes the `src` directory when calling the script directly. The examples below set it inline; the `make surface_stations_preprocess` target also takes care of this.
- The raw station files must exist under the paths defined in `config/globals.py` (for example `data/ws/inmet/`).

## 2. Station System Configuration

Preprocessing behaviour is controlled by JSON files under `config/station_systems/`. Each file corresponds to a station system (INMET, AlertaRio, etc.) and provides:

| Key | Description |
| --- | --- |
| `column_mapping` | Maps raw column names to the canonical names expected by the pipeline. |
| `features` | Toggles that enable or skip feature engineering steps (`add_wind_related_features`, `add_hour_related_features`, `normalize_predictors`, `impute_missing_values`). |
| `predictor_columns` | Ordered list of predictor columns to retain after feature engineering. |
| `stations` | Metadata indexed by station id (name, state, status, coordinates, altitude, start date). |
| `quality` | Optional quality-control checks (bounds, IQR/z-score detection, flag generation). |
| `preprocessing` | Optional scaling/imputation configuration (`scaler`, `imputation`). |
| `target_column` | Name of the target variable to keep with the predictors. |

Example (`config/station_systems/inmet.json`):
```json
{
  "column_mapping": {
    "datetime": "datetime",
    "TEM_MAX": "temperature",
    "UMD_MAX": "relative_humidity",
    "PRE_MAX": "barometric_pressure",
    "VEN_VEL": "wind_speed",
    "VEN_DIR": "wind_dir",
    "CHUVA": "precipitation"
  },
  "features": {
    "add_wind_related_features": true,
    "add_hour_related_features": true,
    "normalize_predictors": true,
    "impute_missing_values": true
  },
  "predictor_columns": [
    "temperature",
    "barometric_pressure",
    "relative_humidity",
    "wind_direction_u",
    "wind_direction_v",
    "hour_sin",
    "hour_cos"
  ],
  "stations": {
    "A601": {
      "name": "SEROPEDICA-ECOLOGIA AGRICOLA",
      "state": "RJ",
      "status": "Operante",
      "latitude": -22.75777777,
      "longitude": -43.68472221,
      "altitude_m": 35.0,
      "operation_start": "2000-05-23"
    }
  },
  "target_column": "precipitation",
  "quality": {
    "enabled": true,
    "flag_column_suffix": "_flag",
    "include_flag_columns": true,
    "checks": [
      {
        "type": "bounds",
        "name": "temperature_physical_limits",
        "columns": ["temperature"],
        "min": -60,
        "max": 60,
        "actions": ["clip", "flag"]
      },
      {
        "type": "bounds",
        "name": "humidity_range",
        "columns": ["relative_humidity"],
        "min": 0,
        "max": 100,
        "actions": ["clip", "flag"]
      }
    ]
  },
  "preprocessing": {
    "scaler": {
      "type": "minmax",
      "params": {
        "feature_range": [0, 1]
      }
    },
    "imputation": {
      "strategy": "knn",
      "params": {
        "n_neighbors": 2,
        "weights": "uniform"
      }
    }
  },
  "report": {
    "enabled": true,
    "fields": ["missing_fraction", "imputed_fraction", "min", "max"],
    "summary": ["rows", "columns", "missing_fraction", "imputed_fraction"]
  }
}
```

To add a new system, duplicate one of the existing JSON files, adjust the mapping and toggles, then reference the new system from the command line (see Section 3).

## 3. Command-Line Interface

`preprocess.py` expects at least the station identifier. You can optionally specify the station system; otherwise the script infers it from `config/globals.py`.

```text
usage: preprocess.py -s STATION_ID [-y STATION_SYSTEM]

options:
  -s, --station_id       Station code to preprocess (required).
  -y, --station_system   Station system name (optional). Accepted values are the keys defined in `STATION_SYSTEM_CONFIG` inside `preprocess.py` (currently `INMET`, `ALERTARIO`).
```

### Example (direct invocation)
```bash
PYTHONPATH=src python3 src/surface_stations/preprocess.py \
  --station_id A601 \
  --station_system INMET
```

### Example (Make target)
```bash
make surface_stations_preprocess \
  STATION_ID=A601 \
  SYSTEM=INMET
```

## 4. Processing Steps

For stations whose system is configured via JSON, the script performs:
1. **Timestamp index creation** using `utils.util.add_datetime_index`.
2. **Column standardisation** according to the system's `column_mapping`.
3. **Feature selection** using `predictor_columns` and `target_column`.
4. **Quality checks** (optional) defined in `quality.checks` to clip, flag or nullify outliers before feature creation.
5. **Feature engineering** toggled per system:
   - Wind vector components (`add_wind_related_features`).
   - Time-of-day sine/cosine (`add_hour_related_features`).
   - Min-max normalisation (`normalize_predictors`).
   - KNN-based gap filling (`impute_missing_values`).
6. **Persistence** to `<output_dir>/<station>_preprocessed.parquet.gzip`, where `output_dir` comes from the system definition in `config/globals.py`.
7. **Optional report** (when `report.enabled` is true) saved as `<output_dir>/<station>_preprocess_report.json` with missing-data stats, quality flags and the preprocessing settings applied.

## 5. Quality Filters (`quality`)

Each station system may declare hygiene rules in the `quality` section of its JSON. The structure is:

```json
"quality": {
  "enabled": true,
  "flag_column_suffix": "_flag",
  "include_flag_columns": true,
  "checks": [
    {
      "type": "bounds",
      "name": "temperature_physical_limits",
      "columns": ["temperature"],
      "min": -60,
      "max": 60,
      "actions": ["clip", "flag"]
    }
  ]
}
```

- `enabled`: toggles the entire block on/off. When false, the checks are skipped.
- `flag_column_suffix`: suffix used when creating boolean flag columns via the `flag` action.
- `include_flag_columns`: if true, the generated flag columns are appended to the predictor list.
- `checks`: list of rules applied before feature engineering. Common keys:
  - `type`: supported values:
    - `bounds`: compares against physical limits (`min`, `max`).
    - `iqr`: applies the interquartile range rule with optional factor `k` (default 1.5).
    - `zscore`: flags absolute z-scores above `threshold` (default 3.0).
  - `columns`: explicit list of target columns; defaults to the predictor set.
  - `actions`: ordered actions applied to the flagged rows:
    - `clip`: truncate values to the computed bounds (`bounds`/`iqr` only).
    - `set_nan`: replace flagged values with `NaN` so the imputer can handle them.
    - `flag`: create/update a boolean column (`<column><suffix>`) marking suspect readings.
  - `name`: optional label surfaced in the report for readability.

### Report interpretation

With `report.enabled` set to true, the generated JSON report contains:

- `quality_checks`: one entry per rule with `flags` (count), `fraction` (share of rows affected), `flag_column` (if any) and the actions applied.
- `column_metrics.missing_fraction`: share of missing values before imputation; `imputed_fraction` repeats it when an imputation strategy ran.
- Flag columns (`*_flag`) appear in the Parquet output when `include_flag_columns` is enabled, allowing downstream filtering or robust modelling.

### Quick tips
- Adjust physical limits (e.g., temperature, humidity, wind) per network reality.
- Combine `clip` + `flag` to keep a truncated value while still marking the observation.
- Use `set_nan` together with imputation when you prefer to discard the raw measurement.
- Checks run before normalisation and imputation, so values turned into `NaN` are handled by the configured imputer.

Warnings are logged if mapped columns or predictors are missing, and the run aborts when the target column cannot be found.

## 5. Tips and Troubleshooting
- Run with `--station_system` whenever you introduce a new JSON so that the script does not rely solely on inference.
- If you encounter `ModuleNotFoundError: No module named 'config'`, verify that `PYTHONPATH` includes `src` or call the Make target.
- To disable a processing step (for example, wind features on gauge-only stations), set the corresponding flag to `false` in the system JSON.
- After editing JSON files, keep them valid by running a quick lint (`python -m json.tool config/station_systems/<file>.json`).
- Customise the generated report by editing `report.fields` (per-column metrics) and `report.summary` (dataset-level metrics) in each system JSON.
- Combine `quality.checks` and the `actions` list (`flag`, `clip`, `set_nan`) to enforce physical limits or highlight suspect readings before feature engineering.
