import argparse
import json
import logging
import sys
from functools import lru_cache
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import pandas as pd
from sklearn.impute import KNNImputer
from sklearn.preprocessing import MinMaxScaler, StandardScaler

from config import globals
from utils import util as util

STATION_SYSTEM_CONFIG_DIR = PROJECT_ROOT / "config" / "station_systems"

STATION_SYSTEM_CONFIG = {
    'inmet': {
        'ids': set(globals.INMET_WEATHER_STATION_IDS),
        'data_dir': globals.WS_INMET_DATA_DIR,
        'config_path': STATION_SYSTEM_CONFIG_DIR / "inmet.json",
    },
    'alertario': {
        'ids': set(globals.ALERTARIO_WEATHER_STATION_IDS),
        'data_dir': globals.WS_ALERTARIO_DATA_DIR,
        'config_path': STATION_SYSTEM_CONFIG_DIR / "alertario.json",
    },
}


@lru_cache(maxsize=None)
def load_station_system_settings(system_name: str) -> dict:
    system_name = system_name.lower()
    if system_name not in STATION_SYSTEM_CONFIG:
        raise KeyError(f"Unknown station system '{system_name}'.")
    config_path = STATION_SYSTEM_CONFIG[system_name]['config_path']
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found for system '{system_name}' at '{config_path}'.")
    with config_path.open('r', encoding='utf-8') as config_file:
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
        logging.warning(f"Invalid imputation strategy '{strategy}'. Skipping imputation.")
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

    logging.info(f"Loading datasource file {ws_filename}).")
    df = pd.read_parquet(ws_filename)
    logging.info(df.head())
    logging.info("Done!\n")

    #
    # Add index to dataframe using the timestamps.
    logging.info(f"Adding index to dataframe using the timestamps...")
    df = util.add_datetime_index(ws_id, df)
    logging.info(df.head())
    logging.info("Done!\n")

    #
    # Standardize column names.
    logging.info(f"Standardizing column names...")
    column_name_mapping = system_settings.get("column_mapping", {})
    if not column_name_mapping:
        raise ValueError(f"No column mapping provided for station system '{station_system}'.")
    missing_columns = [col for col in column_name_mapping if col not in df.columns]
    if missing_columns:
        logging.warning(f"The following columns from the mapping are missing in the datasource: {missing_columns}")
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
            raise ValueError(f"Could not determine predictors/target for station '{ws_id}'.")
        predictor_names, target_name = variables
    logging.info(f"Predictors: {predictor_names}")
    logging.info(f"Target: {target_name}")
    logging.info("Done!\n")

    #
    # Drop observations in which the target variable is not defined.
    logging.info(f"Dropping entries with null target...")
    n_obser_before_drop = len(df)
    df = df[df[target_name].notna()]
    n_obser_after_drop = len(df)
    logging.info(f"Number of observations before/after dropping entries with undefined target value: {n_obser_before_drop}/{n_obser_after_drop}.")
    logging.info(f"Range of timestamps after dropping entries with undefined target value: [{min(df.index)}, {max(df.index)}]")
    logging.info(df.head())
    logging.info("Done!\n")

    #
    # Create wind-related features (U and V components of wind observations).
    if add_wind_features:
        logging.info(f"Creating wind-related features...")
        df = util.add_wind_related_features(ws_id, df)
        logging.info(df.head())
        logging.info("Done!\n")
    else:
        logging.info("Skipping wind-related feature generation as configured.\n")

    #
    # Create time-related features (sin and cos components)
    if add_hour_features:
        logging.info(f"Creating time-related features...")
        df = util.add_hour_related_features(df)
        logging.info(df.head())
        logging.info("Done!\n")
    else:
        logging.info("Skipping time-related feature generation as configured.\n")

    available_predictors = [column for column in predictor_names if column in df.columns]
    missing_predictors = sorted(set(predictor_names) - set(available_predictors))
    if missing_predictors:
        logging.warning(f"The following predictors are missing and will be ignored: {missing_predictors}")
    if not available_predictors:
        raise ValueError(f"No predictor columns were found after preprocessing for station '{ws_id}'.")
    if target_name not in df.columns:
        raise KeyError(f"Target column '{target_name}' not found after preprocessing for station '{ws_id}'.")
    df = df[available_predictors + [target_name]]

    #
    # Scale the data. This step is necessary here due to the next step, which deals with missing values.
    # Notice that we drop the target column before scaling, to avoid some kind of data leakage.
    # (see https://stats.stackexchange.com/questions/214728/should-data-be-normalized-before-or-after-imputation-of-missing-data)
    target_column = df[target_name]
    predictors_df = df.drop(columns=[target_name], axis=1).copy()
    if normalize_predictors:
        scaler_type = (scaler_config.get("type") or "minmax") if isinstance(scaler_config, dict) else "minmax"
        logging.info(f"Applying '{scaler_type}' scaling...")
        predictors_df = apply_scaling(predictors_df, scaler_config)
        logging.info("Done!\n")
    else:
        logging.info("Skipping normalization as configured.\n")

    # 
    # Imput missing values on some features.
    if impute_missing_values:
        strategy = (imputation_config.get("strategy") or "knn") if isinstance(imputation_config, dict) else "knn"
        logging.info(f"Applying '{strategy}' imputation...")
        percentage_missing = (predictors_df.isna().mean() * 100).mean() # Compute the percentage of missing values
        logging.info(f"There are {predictors_df.isnull().sum().sum()} missing values ({percentage_missing:.2f}%). Going to fill them...")
        predictors_df = apply_imputation(predictors_df, imputation_config)
        assert (not predictors_df.isnull().values.any().any())
        logging.info("Done!\n")
    else:
        logging.info("Skipping missing-value imputation as configured.\n")

    #
    # Add the target column back to the DataFrame.
    predictors_df[target_name] = target_column
    df = predictors_df

    #
    # Save preprocessed data to a parquet file.
    filename_and_extension = util.get_filename_and_extension(ws_filename)
    filename = output_folder + filename_and_extension[0] + '_preprocessed.parquet.gzip'
    logging.info(f"Saving preprocessed data to {filename}...")
    df.to_parquet(filename, compression='gzip')
    logging.info("Done!\n")

    logging.info("Done it all!\n")

def main(argv):
    parser = argparse.ArgumentParser(description='Preprocess weather station data.')
    parser.add_argument('-s', '--station_id', 
                        required=True, 
                        choices=globals.INMET_WEATHER_STATION_IDS + globals.ALERTARIO_WEATHER_STATION_IDS, 
                        help='ID of the weather station to preprocess data for.')
    parser.add_argument('-y', '--station_system',
                        choices=sorted(STATION_SYSTEM_CONFIG.keys()),
                        type=str.lower,
                        help='System of the weather station (e.g., INMET, AlertaRio).')
    args = parser.parse_args(argv[1:])
    
    station_id = args.station_id
    station_system = args.station_system

    fmt = "[%(levelname)s] %(funcName)s():%(lineno)i: %(message)s"
    logging.basicConfig(level=logging.DEBUG, format = fmt)

    if station_system:
        system_config = STATION_SYSTEM_CONFIG[station_system]
        if station_id not in system_config['ids']:
            parser.error(f"Station {station_id} does not belong to the {station_system.upper()} system.")
        ws_data_dir = system_config['data_dir']
    else:
        ws_data_dir = None
        for system_name, system_config in STATION_SYSTEM_CONFIG.items():
            if station_id in system_config['ids']:
                station_system = system_name
                ws_data_dir = system_config['data_dir']
                break
        if ws_data_dir is None:
            parser.error(f"Invalid station identifier: {station_id}")

    print(f'Preprocessing data coming from weather station {station_id} (system: {station_system.upper()})')
    ws_filename = ws_data_dir + args.station_id + ".parquet"
    preprocess_ws(ws_id=station_id, ws_filename=ws_filename, output_folder=ws_data_dir, station_system=station_system)

    print('Done!')

if __name__ == '__main__':
    main(sys.argv)
