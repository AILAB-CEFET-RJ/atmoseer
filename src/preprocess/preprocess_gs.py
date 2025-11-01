import argparse
import sys

import pandas as pd

import src.utils.util as util
from config import globals


def get_relevant_variables():
    return [
        "temperature",
        "relative_humidity",
        "barometric_pressure",
        "wind_direction_u",
        "wind_direction_v",
        "hour_sin",
        "hour_cos",
    ], "precipitation"


def preprocess_gauge_station(station_id, df, output_folder):
    predictor_names, target_name = get_relevant_variables()
    print(f"Chosen predictors: {predictor_names}")
    print(f"Chosen target: {target_name}")

    #
    # Drop observations in which the target variable is not defined.
    print("Dropping entries with null target.")
    n_obser_before_drop = len(df)
    df = df[df[target_name].notna()]
    n_obser_after_drop = len(df)
    print(
        f"Number of observations before/after dropping entries with undefined target value: {n_obser_before_drop}/{n_obser_after_drop}."
    )
    print(
        f"Range of timestamps after dropping entries with undefined target value: [{min(df.index)}, {max(df.index)}]"
    )

    #
    # Create hour-related features (sin and cos components)
    df = util.add_hour_related_features(df)

    df = df[predictor_names + [target_name]]

    # print(df.head())

    assert not df.isnull().values.any().any()

    #
    # Normalize the weather station data. This step is necessary here due to the next step, which deals with missing values.
    # Notice that we drop the target column before normalizing, to avoid some kind of data leakage.
    # (see https://stats.stackexchange.com/questions/214728/should-data-be-normalized-before-or-after-imputation-of-missing-data)
    print("Min-max normalizing data...", end="")
    target_column = df[target_name]
    barometric_pressure_column = df["barometric_pressure"]
    df = df.drop(columns=[target_name, "barometric_pressure"], axis=1)
    df = util.min_max_normalize(df)
    print("Done!")

    assert not df.isnull().values.any().any()

    #
    # Add the target column back to the DataFrame.
    df["barometric_pressure"] = barometric_pressure_column
    df[target_name] = target_column

    #
    # Save preprocessed data to a parquet file.
    filename = output_folder + station_id + "_preprocessed.parquet.gzip"
    print(f"Saving preprocessed data to {filename}")
    df.to_parquet(filename, compression="gzip")


def preprocess_all_gauge_stations(output_folder):
    alertario_stations_filename = "./data/ws/alertario_stations.parquet"
    df_alertario_stations = pd.read_parquet(alertario_stations_filename)
    for index, row in df_alertario_stations.iterrows():
        station_id = row["estacao_desc"]
        if station_id in globals.ALERTARIO_WEATHER_STATION_IDS:
            continue
        print(f"Fusing data for gauge station {station_id}")
        df_station = pd.read_parquet(
            "./data/ws/alertario/rain_gauge_era5_fused/" + station_id + ".parquet"
        )
        preprocess_gauge_station(station_id, df_station, output_folder=output_folder)


def main(argv):
    parser = argparse.ArgumentParser(description="Preprocess gauge station data.")
    parser.add_argument(
        "-s",
        "--station_id",
        required=True,
        choices=globals.ALERTARIO_GAUGE_STATION_IDS + ("all",),
        help="ID of the weather station to preprocess data for.",
    )
    args = parser.parse_args(argv[1:])

    station_id = args.station_id

    if not (station_id == "all" or station_id in globals.ALERTARIO_GAUGE_STATION_IDS):
        print(f"Invalid station identifier: {station_id}")
        parser.print_help()
        sys.exit(2)

    input_folder = "./data/ws/alertario/rain_gauge_era5_fused/"
    output_folder = "./data/ws/alertario/rain_gauge_era5_fused/"

    if station_id == "all":
        preprocess_all_gauge_stations(output_folder)
    else:
        print(f"Preprocessing data coming from weather station {station_id}")
        station_filename = input_folder + args.station_id + ".parquet"
        df = pd.read_parquet(station_filename)
        print(f"Fusing data for gauge station {station_id}")
        preprocess_gauge_station(station_id, df, output_folder=output_folder)

    print("Done!")


if __name__ == "__main__":
    main(sys.argv)
