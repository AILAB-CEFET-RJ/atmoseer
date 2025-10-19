import pandas as pd
from pathlib import Path
import argparse
import sys

from config import globals

from utils import util as util

from sklearn.impute import KNNImputer
import logging

# list_valid_datasource_combinations = ("R", "N", "R+N")

# def preprocess_sounding_data(sounding_data_source):
#     print(f"Loading datasource file ({sounding_data_source}).")
#     df = pd.read_parquet(sounding_data_source)
#     format_string = '%Y-%m-%d %H:%M:%S'

#     #
#     # Add index to dataframe using the observation's timestamps.
#     df['Datetime'] = pd.to_datetime(df['time'], format=format_string)
#     df = df.set_index(pd.DatetimeIndex(df['Datetime']))
#     print(f"Range of timestamps after preprocessing {sounding_data_source}: [{min(df.index)}, {max(df.index)}]")

#     #
#     # Remove time-related columns since now this information is in the index.
#     df = df.drop(['time', 'Datetime'], axis = 1)

#     #
#     # Save preprocessed data.
#     filename_and_extension = get_filename_and_extension(sounding_data_source)
#     filename = WS_DATA_DIR + filename_and_extension[0] + '_preprocessed.parquet.gzip'
#     print(f"Saving preprocessed data to {filename}")
#     df.to_parquet(filename, compression='gzip')

# def preprocess_numerical_model_data(numerical_model_data_source):
#     print(f"Loading NWP datasource file ({numerical_model_data_source}).")
#     df = pd.read_csv(numerical_model_data_source)

#     #
#     # Add index to dataframe using the timestamps.
#     format_string = '%Y-%m-%d %H:%M:%S'
#     df['Datetime'] = pd.to_datetime(df['time'], format=format_string)
#     df = df.set_index(pd.DatetimeIndex(df['Datetime']))
#     df = df.drop(['time', 'Datetime', 'Unnamed: 0'], axis = 1)
#     print(f"Range of timestamps after preprocessing {numerical_model_data_source}: [{min(df.index)}, {max(df.index)}]")

#     df = util.min_max_normalize(df)

#     #
#     # Save preprocessed data.
#     filename_and_extension = get_filename_and_extension(numerical_model_data_source)
#     filename = filename_and_extension[0] + '_preprocessed.parquet.gzip'
#     print(f"Saving preprocessed data to {filename}")
#     df.to_parquet(filename, compression='gzip')

def preprocess_ws(ws_id, ws_filename, output_folder):

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
    if ws_id in globals.ALERTARIO_WEATHER_STATION_IDS:
        column_name_mapping = {
            "datetime": "datetime",
            "temperature_mean": "temperature",
            "humidity_mean": "relative_humidity",
            "pressure_mean": "barometric_pressure",
            "wind_speed_mean": "wind_speed",
            "wind_dir_mean": "wind_dir",
            "precipitation_sum": "precipitation"
        }
    elif ws_id in globals.INMET_WEATHER_STATION_IDS:
        column_name_mapping = {
            "datetime": "datetime",
            "TEM_MAX": "temperature",
            "UMD_MAX": "relative_humidity",
            "PRE_MAX": "barometric_pressure",
            "VEN_VEL": "wind_speed",
            "VEN_DIR": "wind_dir",
            "CHUVA": "precipitation"
        }
    column_names = column_name_mapping.keys()
    df = util.get_dataframe_with_selected_columns(df, column_names)
    df = util.rename_dataframe_column_names(df, column_name_mapping)
    logging.info(df.head())
    logging.info("Done!\n")

    logging.info("Getting relevant variables...")
    predictor_names, target_name = util.get_relevant_variables(ws_id)
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
    logging.info(f"Creating wind-related features...")
    df = util.add_wind_related_features(ws_id, df)
    logging.info(df.head())
    logging.info("Done!\n")

    #
    # Create time-related features (sin and cos components)
    logging.info(f"Creating time-related features...")
    df = util.add_hour_related_features(df)
    logging.info(df.head())
    logging.info("Done!\n")

    df = df[predictor_names + [target_name]]

    #
    # Normalize the weather station data. This step is necessary here due to the next step, which deals with missing values.
    # Notice that we drop the target column before normalizing, to avoid some kind of data leakage.
    # (see https://stats.stackexchange.com/questions/214728/should-data-be-normalized-before-or-after-imputation-of-missing-data)
    logging.info("Min-max normalizing data...")
    target_column = df[target_name]
    df = df.drop(columns=[target_name], axis=1)
    df = util.min_max_normalize(df)
    logging.info("Done!\n")

    # 
    # Imput missing values on some features.
    logging.info("Applying KNNImputer...")
    percentage_missing = (df.isna().mean() * 100).mean() # Compute the percentage of missing values
    logging.info(f"There are {df.isnull().sum().sum()} missing values ({percentage_missing:.2f}%). Going to fill them...")
    imputer = KNNImputer(n_neighbors=2)
    df[:] = imputer.fit_transform(df)
    assert (not df.isnull().values.any().any())
    logging.info("Done!\n")

    #
    # Add the target column back to the DataFrame.
    df[target_name] = target_column

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
    args = parser.parse_args(argv[1:])
    
    station_id = args.station_id

    fmt = "[%(levelname)s] %(funcName)s():%(lineno)i: %(message)s"
    logging.basicConfig(level=logging.DEBUG, format = fmt)

    if not ((station_id in globals.INMET_WEATHER_STATION_IDS) or (station_id in globals.ALERTARIO_WEATHER_STATION_IDS)):
        print(f"Invalid station identifier: {station_id}")
        parser.print_help()
        sys.exit(2)

    if (station_id in globals.INMET_WEATHER_STATION_IDS):
        ws_data_dir = globals.WS_INMET_DATA_DIR
    elif (station_id in globals.ALERTARIO_WEATHER_STATION_IDS):
        ws_data_dir = globals.WS_ALERTARIO_DATA_DIR

    print(f'Preprocessing data coming from weather station {station_id}')
    ws_filename = ws_data_dir + args.station_id + ".parquet"
    preprocess_ws(ws_id=station_id, ws_filename=ws_filename, output_folder=ws_data_dir)

    print('Done!')

if __name__ == '__main__':
    main(sys.argv)
