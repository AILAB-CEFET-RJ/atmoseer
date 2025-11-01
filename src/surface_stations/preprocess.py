import argparse
import json
import logging
import math
import sys
from functools import lru_cache
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import numpy as np
import pandas as pd
from sklearn.impute import KNNImputer
from sklearn.preprocessing import MinMaxScaler, StandardScaler

from config import globals
from utils import util as util


def _safe_get(dct: dict, key: str, default=None):
    try:
        return dct.get(key, default)
    except AttributeError:
        return default


def _is_truthy(value) -> bool:
    if isinstance(value, str):
        return value.lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _nan_to_none(value):
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def _ensure_list(value, default=None):
    if value is None:
        return default or []
    if isinstance(value, (list, tuple)):
        return list(value)
    return [value]


STATION_SYSTEM_CONFIG_DIR = PROJECT_ROOT / "config" / "station_systems"

STATION_SYSTEM_CONFIG = {
    "inmet": {
        "ids": set(globals.INMET_WEATHER_STATION_IDS),
        "data_dir": globals.WS_INMET_DATA_DIR,
        "config_path": STATION_SYSTEM_CONFIG_DIR / "inmet.json",
    },
    "alertario": {
        "ids": set(globals.ALERTARIO_WEATHER_STATION_IDS),
        "data_dir": globals.WS_ALERTARIO_DATA_DIR,
        "config_path": STATION_SYSTEM_CONFIG_DIR / "alertario.json",
    },
}


@lru_cache(maxsize=None)
def load_station_system_settings(system_name: str) -> dict:
    system_name = system_name.lower()
    if system_name not in STATION_SYSTEM_CONFIG:
        raise KeyError(f"Unknown station system '{system_name}'.")
    config_path = STATION_SYSTEM_CONFIG[system_name]["config_path"]
    if not config_path.exists():
        raise FileNotFoundError(
            f"Configuration file not found for system '{system_name}' at '{config_path}'."
        )
    with config_path.open("r", encoding="utf-8") as config_file:
        return json.load(config_file)


def apply_scaling(df: pd.DataFrame, scaler_config: dict) -> pd.DataFrame:
    if df.empty:
        return df
    scaler_config = scaler_config or {}
    scaler_type = scaler_config.get("type", "minmax")
    if not isinstance(scaler_type, str):
        logging.warning(f"Invalid scaler type '{scaler_type}'. Skipping normalization.")
        return df
    scaler_type = scaler_type.lower()
    params = scaler_config.get("params", {}) or {}

    if scaler_type == "minmax":
        feature_range = params.get("feature_range", [0.0, 1.0])
        if feature_range == [0.0, 1.0] or tuple(feature_range) == (0.0, 1.0):
            return util.min_max_normalize(df)
        scaler = MinMaxScaler(feature_range=tuple(feature_range))
        scaled = scaler.fit_transform(df)
    elif scaler_type == "standard":
        scaler = StandardScaler(**params)
        scaled = scaler.fit_transform(df)
    elif scaler_type in {"none", "identity"}:
        return df
    else:
        logging.warning(f"Unknown scaler type '{scaler_type}'. Skipping normalization.")
        return df

    scaled_df = pd.DataFrame(scaled, columns=df.columns, index=df.index)
    return scaled_df


def apply_imputation(df: pd.DataFrame, imputation_config: dict) -> pd.DataFrame:
    if df.empty:
        return df
    imputation_config = imputation_config or {}
    strategy = imputation_config.get("strategy", "knn")
    if not isinstance(strategy, str):
        logging.warning(
            f"Invalid imputation strategy '{strategy}'. Skipping imputation."
        )
        return df
    strategy = strategy.lower()
    params = imputation_config.get("params", {}) or {}

    if strategy == "knn":
        n_neighbors = params.get("n_neighbors", 2)
        weights = params.get("weights", "uniform")
        imputer = KNNImputer(n_neighbors=n_neighbors, weights=weights)
        imputed = imputer.fit_transform(df)
        return pd.DataFrame(imputed, columns=df.columns, index=df.index)
    if strategy in {"ffill_then_zero", "forward_then_zero"}:
        return df.ffill().fillna(0.0)
    if strategy in {"ffill", "forward"}:
        return df.ffill()
    if strategy in {"bfill", "backward"}:
        return df.bfill()
    if strategy in {"zero", "zeros"}:
        return df.fillna(0.0)
    if strategy in {"none", "skip"}:
        return df

    logging.warning(f"Unknown imputation strategy '{strategy}'. Skipping imputation.")
    return df


def apply_quality_checks(
    df: pd.DataFrame,
    quality_config: dict,
    predictor_columns,
    flag_suffix: str = "_flag",
    include_flag_columns: bool = True,
):
    if df.empty:
        return df, {"checks": [], "flag_columns": [], "new_columns": []}

    checks = _ensure_list(_safe_get(quality_config, "checks"), default=[])
    records = []
    created_flag_columns = []
    new_predictor_columns = []
    row_count = len(df.index)

    for idx, raw_check in enumerate(checks):
        check = raw_check or {}
        check_type = str(check.get("type", "")).lower()
        if not check_type:
            continue
        columns = _ensure_list(check.get("columns"), default=predictor_columns)
        actions = _ensure_list(
            check.get("actions") or check.get("strategy"), default=["flag"]
        )
        actions = [action.lower() for action in actions]
        if not actions:
            continue

        # compute parameters common across columns depending on check type
        for column in columns:
            if column not in df.columns:
                logging.debug(
                    f"Quality check '{check_type}' skipped for missing column '{column}'."
                )
                continue
            series = df[column]
            if not pd.api.types.is_numeric_dtype(series):
                logging.debug(
                    f"Quality check '{check_type}' skipped for non-numeric column '{column}'."
                )
                continue

            mask = pd.Series(False, index=df.index)
            params_summary = {}

            if check_type == "bounds":
                min_value = check.get("min")
                max_value = check.get("max")
                if min_value is not None:
                    mask |= series < min_value
                if max_value is not None:
                    mask |= series > max_value
                params_summary = {"min": min_value, "max": max_value}
            elif check_type == "iqr":
                k = float(check.get("k", 1.5))
                q1 = series.quantile(0.25)
                q3 = series.quantile(0.75)
                iqr = q3 - q1
                if pd.isna(iqr) or iqr == 0:
                    continue
                lower = q1 - k * iqr
                upper = q3 + k * iqr
                mask = (series < lower) | (series > upper)
                params_summary = {"k": k, "lower": lower, "upper": upper}
            elif check_type == "zscore":
                threshold = float(check.get("threshold", 3.0))
                mean = series.mean()
                std = series.std()
                if pd.isna(std) or std == 0:
                    continue
                zscores = (series - mean) / std
                mask = zscores.abs() > threshold
                params_summary = {"threshold": threshold}
            else:
                logging.warning(
                    f"Unknown quality check type '{check_type}' for column '{column}'. Skipping."
                )
                continue

            if not mask.any():
                records.append(
                    {
                        "check": check.get("name") or f"{check_type}_{idx}",
                        "type": check_type,
                        "column": column,
                        "flags": 0,
                        "fraction": 0.0,
                        "actions": actions,
                        "params": params_summary,
                    }
                )
                continue

            if "clip" in actions and check_type in {"bounds", "iqr"}:
                if "min" in params_summary and params_summary["min"] is not None:
                    df.loc[series < params_summary["min"], column] = params_summary[
                        "min"
                    ]
                if "max" in params_summary and params_summary["max"] is not None:
                    df.loc[series > params_summary["max"], column] = params_summary[
                        "max"
                    ]
                if "lower" in params_summary and params_summary["lower"] is not None:
                    df.loc[series < params_summary["lower"], column] = params_summary[
                        "lower"
                    ]
                if "upper" in params_summary and params_summary["upper"] is not None:
                    df.loc[series > params_summary["upper"], column] = params_summary[
                        "upper"
                    ]

            if "set_nan" in actions:
                df.loc[mask, column] = np.nan

            flag_column = None
            if "flag" in actions:
                flag_column = check.get("flag_column") or f"{column}{flag_suffix}"
                if flag_column not in df.columns:
                    df[flag_column] = False
                df[flag_column] = df[flag_column].fillna(False).astype(bool)
                df.loc[mask, flag_column] = True
                df[flag_column] = df[flag_column].astype(bool)
                created_flag_columns.append(flag_column)
                if include_flag_columns:
                    if flag_column not in new_predictor_columns:
                        new_predictor_columns.append(flag_column)

            records.append(
                {
                    "check": check.get("name") or f"{check_type}_{idx}",
                    "type": check_type,
                    "column": column,
                    "flags": int(mask.sum()),
                    "fraction": float(mask.sum()) / row_count if row_count else 0.0,
                    "actions": actions,
                    "flag_column": flag_column,
                    "params": params_summary,
                }
            )

    quality_output = {
        "checks": records,
        "flag_columns": list(dict.fromkeys(created_flag_columns)),
        "new_columns": list(dict.fromkeys(new_predictor_columns)),
    }
    return df, quality_output


def build_quality_report(
    station_id: str,
    station_system: str,
    report_df: pd.DataFrame,
    report_config: dict,
    station_metadata: dict,
    scaler_config: dict,
    imputation_config: dict,
    missing_counts_before: pd.Series,
    imputation_applied: bool,
    source_file: str,
    output_file: str,
    target_column: str,
    quality_checks=None,
) -> dict:
    report_fields = _ensure_list(
        _safe_get(report_config, "fields"), default=["missing_fraction", "min", "max"]
    )
    summary_keys = _ensure_list(
        _safe_get(report_config, "summary"),
        default=["rows", "columns", "missing_fraction"],
    )

    num_rows = int(report_df.shape[0])
    num_columns = int(report_df.shape[1])
    total_cells = num_rows * num_columns if num_rows and num_columns else 0
    missing_fraction = (
        (missing_counts_before.sum() / total_cells) if total_cells else 0.0
    )

    per_column_metrics = {}
    for column in report_df.columns:
        column_metrics = {}
        for field in report_fields:
            if field == "missing_fraction":
                column_metrics[field] = (
                    float(missing_counts_before[column]) / num_rows if num_rows else 0.0
                )
            elif field == "missing_count":
                column_metrics[field] = int(missing_counts_before[column])
            elif field == "imputed_fraction":
                column_metrics[field] = (
                    float(missing_counts_before[column]) / num_rows
                    if (num_rows and imputation_applied)
                    else 0.0
                )
            elif field == "imputed_count":
                column_metrics[field] = (
                    int(missing_counts_before[column]) if imputation_applied else 0
                )
            elif field == "min":
                column_metrics[field] = _nan_to_none(report_df[column].min(skipna=True))
            elif field == "max":
                column_metrics[field] = _nan_to_none(report_df[column].max(skipna=True))
            elif field == "mean":
                column_metrics[field] = _nan_to_none(
                    report_df[column].mean(skipna=True)
                )
            elif field == "median":
                column_metrics[field] = _nan_to_none(
                    report_df[column].median(skipna=True)
                )
            elif field == "std":
                column_metrics[field] = _nan_to_none(report_df[column].std(skipna=True))
        per_column_metrics[column] = column_metrics

    summary = {}
    for key in summary_keys:
        if key == "rows":
            summary[key] = num_rows
        elif key == "columns":
            summary[key] = num_columns
        elif key == "missing_fraction":
            summary[key] = missing_fraction
        elif key == "imputed_fraction":
            summary[key] = missing_fraction if imputation_applied else 0.0
        elif key == "min":
            summary[key] = _nan_to_none(report_df.min(skipna=True).min())
        elif key == "max":
            summary[key] = _nan_to_none(report_df.max(skipna=True).max())
        elif key == "mean":
            summary[key] = _nan_to_none(report_df.mean(skipna=True).mean())

    report_payload = {
        "station_id": station_id,
        "station_system": station_system,
        "target_column": target_column,
        "columns": list(report_df.columns),
        "rows": num_rows,
        "metadata": station_metadata or {},
        "scaler": scaler_config or {},
        "imputation": imputation_config or {},
        "imputation_applied": imputation_applied,
        "source_file": source_file,
        "output_file": output_file,
        "column_metrics": per_column_metrics,
        "summary": summary,
        "quality_checks": quality_checks or [],
    }
    return report_payload


def preprocess_ws(ws_id, ws_filename, output_folder, station_system):
    system_settings = load_station_system_settings(station_system)
    feature_toggles = system_settings.get("features", {})
    add_wind_features = feature_toggles.get("add_wind_related_features", True)
    add_hour_features = feature_toggles.get("add_hour_related_features", True)
    normalize_predictors = feature_toggles.get("normalize_predictors", True)
    impute_missing_values = feature_toggles.get("impute_missing_values", True)
    preprocessing_settings = system_settings.get("preprocessing", {})
    scaler_config = preprocessing_settings.get("scaler", {})
    imputation_config = preprocessing_settings.get("imputation", {})
    quality_config = system_settings.get("quality", {})
    quality_enabled = _is_truthy(_safe_get(quality_config, "enabled", False))
    include_flag_columns = _is_truthy(
        _safe_get(quality_config, "include_flag_columns", True)
    )
    flag_suffix = quality_config.get("flag_column_suffix", "_flag")
    report_config = system_settings.get("report", {})
    report_enabled = _is_truthy(_safe_get(report_config, "enabled", False))
    station_metadata = _safe_get(_safe_get(system_settings, "stations", {}), ws_id, {})

    logging.info(f"Loading datasource file {ws_filename}).")
    df = pd.read_parquet(ws_filename)
    logging.info(df.head())
    logging.info("Done!\n")

    #
    # Add index to dataframe using the timestamps.
    logging.info("Adding index to dataframe using the timestamps...")
    df = util.add_datetime_index(ws_id, df)
    logging.info(df.head())
    logging.info("Done!\n")

    #
    # Standardize column names.
    logging.info("Standardizing column names...")
    column_name_mapping = system_settings.get("column_mapping", {})
    if not column_name_mapping:
        raise ValueError(
            f"No column mapping provided for station system '{station_system}'."
        )
    missing_columns = [col for col in column_name_mapping if col not in df.columns]
    if missing_columns:
        logging.warning(
            f"The following columns from the mapping are missing in the datasource: {missing_columns}"
        )
    column_names = column_name_mapping.keys()
    df = util.get_dataframe_with_selected_columns(df, column_names)
    df = util.rename_dataframe_column_names(df, column_name_mapping)
    logging.info(df.head())
    logging.info("Done!\n")

    logging.info("Getting relevant variables...")
    predictor_names = system_settings.get("predictor_columns")
    target_name = system_settings.get("target_column")
    if not (predictor_names and target_name):
        variables = util.get_relevant_variables(ws_id)
        if not variables:
            raise ValueError(
                f"Could not determine predictors/target for station '{ws_id}'."
            )
        predictor_names, target_name = variables
    logging.info(f"Predictors: {predictor_names}")
    logging.info(f"Target: {target_name}")
    logging.info("Done!\n")

    #
    # Drop observations in which the target variable is not defined.
    logging.info("Dropping entries with null target...")
    n_obser_before_drop = len(df)
    df = df[df[target_name].notna()]
    n_obser_after_drop = len(df)
    logging.info(
        f"Number of observations before/after dropping entries with undefined target value: {n_obser_before_drop}/{n_obser_after_drop}."
    )
    logging.info(
        f"Range of timestamps after dropping entries with undefined target value: [{min(df.index)}, {max(df.index)}]"
    )
    logging.info(df.head())
    logging.info("Done!\n")

    quality_summary = []
    quality_added_predictors = []
    if quality_enabled:
        logging.info("Applying quality checks before feature engineering...")
        pre_quality_columns = [
            column for column in predictor_names if column in df.columns
        ]
        df, quality_output = apply_quality_checks(
            df=df,
            quality_config=quality_config,
            predictor_columns=pre_quality_columns,
            flag_suffix=flag_suffix,
            include_flag_columns=include_flag_columns,
        )
        quality_summary = quality_output.get("checks", [])
        if include_flag_columns:
            quality_added_predictors = [
                column
                for column in quality_output.get("new_columns", [])
                if column in df.columns
            ]
        logging.info("Quality checks completed.\n")

    #
    # Create wind-related features (U and V components of wind observations).
    if add_wind_features:
        logging.info("Creating wind-related features...")
        df = util.add_wind_related_features(ws_id, df)
        logging.info(df.head())
        logging.info("Done!\n")
    else:
        logging.info("Skipping wind-related feature generation as configured.\n")

    #
    # Create time-related features (sin and cos components)
    if add_hour_features:
        logging.info("Creating time-related features...")
        df = util.add_hour_related_features(df)
        logging.info(df.head())
        logging.info("Done!\n")
    else:
        logging.info("Skipping time-related feature generation as configured.\n")

    available_predictors = [
        column for column in predictor_names if column in df.columns
    ]
    if include_flag_columns and quality_added_predictors:
        for column in quality_added_predictors:
            if column not in available_predictors:
                available_predictors.append(column)
    missing_predictors = sorted(set(predictor_names) - set(available_predictors))
    if missing_predictors:
        logging.warning(
            f"The following predictors are missing and will be ignored: {missing_predictors}"
        )
    if not available_predictors:
        raise ValueError(
            f"No predictor columns were found after preprocessing for station '{ws_id}'."
        )
    if target_name not in df.columns:
        raise KeyError(
            f"Target column '{target_name}' not found after preprocessing for station '{ws_id}'."
        )
    df = df[available_predictors + [target_name]]
    report_reference_df = df[available_predictors].copy() if report_enabled else None
    missing_counts_before = (
        report_reference_df.isna().sum() if report_reference_df is not None else None
    )

    #
    # Scale the data. This step is necessary here due to the next step, which deals with missing values.
    # Notice that we drop the target column before scaling, to avoid some kind of data leakage.
    # (see https://stats.stackexchange.com/questions/214728/should-data-be-normalized-before-or-after-imputation-of-missing-data)
    target_column = df[target_name]
    predictors_df = df.drop(columns=[target_name], axis=1).copy()
    if normalize_predictors:
        scaler_type = (
            (scaler_config.get("type") or "minmax")
            if isinstance(scaler_config, dict)
            else "minmax"
        )
        logging.info(f"Applying '{scaler_type}' scaling...")
        predictors_df = apply_scaling(predictors_df, scaler_config)
        logging.info("Done!\n")
    else:
        logging.info("Skipping normalization as configured.\n")

    #
    # Imput missing values on some features.
    if impute_missing_values:
        strategy = (
            (imputation_config.get("strategy") or "knn")
            if isinstance(imputation_config, dict)
            else "knn"
        )
        logging.info(f"Applying '{strategy}' imputation...")
        percentage_missing = (
            predictors_df.isna().mean() * 100
        ).mean()  # Compute the percentage of missing values
        logging.info(
            f"There are {predictors_df.isnull().sum().sum()} missing values ({percentage_missing:.2f}%). Going to fill them..."
        )
        predictors_df = apply_imputation(predictors_df, imputation_config)
        assert not predictors_df.isnull().values.any().any()
        logging.info("Done!\n")
    else:
        logging.info("Skipping missing-value imputation as configured.\n")
    strategy_value = None
    strategy_flag = True
    if isinstance(imputation_config, dict):
        strategy_value = imputation_config.get("strategy", "knn")
        if isinstance(strategy_value, str):
            strategy_flag = strategy_value.lower() not in {"none", "skip"}
    imputation_applied = bool(impute_missing_values and strategy_flag)

    #
    # Add the target column back to the DataFrame.
    predictors_df[target_name] = target_column
    df = predictors_df

    #
    # Save preprocessed data to a parquet file.
    filename_and_extension = util.get_filename_and_extension(ws_filename)
    filename = output_folder + filename_and_extension[0] + "_preprocessed.parquet.gzip"
    logging.info(f"Saving preprocessed data to {filename}...")
    df.to_parquet(filename, compression="gzip")
    logging.info("Done!\n")

    if (
        report_enabled
        and report_reference_df is not None
        and missing_counts_before is not None
    ):
        report_payload = build_quality_report(
            station_id=ws_id,
            station_system=station_system,
            report_df=report_reference_df,
            report_config=report_config,
            station_metadata=station_metadata,
            scaler_config=scaler_config,
            imputation_config=imputation_config,
            missing_counts_before=missing_counts_before,
            imputation_applied=imputation_applied,
            source_file=ws_filename,
            output_file=filename,
            target_column=target_name,
            quality_checks=quality_summary,
        )
        report_filename = (
            output_folder + filename_and_extension[0] + "_preprocess_report.json"
        )
        with open(report_filename, "w", encoding="utf-8") as report_file:
            json.dump(report_payload, report_file, indent=2, ensure_ascii=False)
        logging.info(f"Quality report saved to {report_filename}\n")

    logging.info("Done it all!\n")


def main(argv):
    parser = argparse.ArgumentParser(description="Preprocess weather station data.")
    parser.add_argument(
        "-s",
        "--station_id",
        required=True,
        choices=globals.INMET_WEATHER_STATION_IDS
        + globals.ALERTARIO_WEATHER_STATION_IDS,
        help="ID of the weather station to preprocess data for.",
    )
    parser.add_argument(
        "-y",
        "--station_system",
        choices=sorted(STATION_SYSTEM_CONFIG.keys()),
        type=str.lower,
        help="System of the weather station (e.g., INMET, AlertaRio).",
    )
    args = parser.parse_args(argv[1:])

    station_id = args.station_id
    station_system = args.station_system

    fmt = "[%(levelname)s] %(funcName)s():%(lineno)i: %(message)s"
    logging.basicConfig(level=logging.DEBUG, format=fmt)

    if station_system:
        system_config = STATION_SYSTEM_CONFIG[station_system]
        if station_id not in system_config["ids"]:
            parser.error(
                f"Station {station_id} does not belong to the {station_system.upper()} system."
            )
        ws_data_dir = system_config["data_dir"]
    else:
        ws_data_dir = None
        for system_name, system_config in STATION_SYSTEM_CONFIG.items():
            if station_id in system_config["ids"]:
                station_system = system_name
                ws_data_dir = system_config["data_dir"]
                break
        if ws_data_dir is None:
            parser.error(f"Invalid station identifier: {station_id}")

    print(
        f"Preprocessing data coming from weather station {station_id} (system: {station_system.upper()})"
    )
    ws_filename = ws_data_dir + args.station_id + ".parquet"
    preprocess_ws(
        ws_id=station_id,
        ws_filename=ws_filename,
        output_folder=ws_data_dir,
        station_system=station_system,
    )


if __name__ == "__main__":
    main(sys.argv)
