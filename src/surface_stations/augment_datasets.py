"""
This script merges the train/val/test datasets from two or more user-specified weather stations
"""

import argparse
import pickle
import sys

import numpy as np
import pandas as pd
from globals import (
    ALERTARIO_WEATHER_STATION_IDS,
    DATASETS_DIR,
    INMET_WEATHER_STATION_IDS,
)

from src.utils.util import haversine_distance
from utils.rainfall import OrdinalPrecipitationLevel, get_events_per_level

# def get_pos_class_ratio(y):
#     pos_examples_idxs = np.where(y > 0)[0]
#     return len(pos_examples_idxs)/len(y)


def print_events_by_level(suffix, y):
    none_idx, weak_idx, moderate_idx, strong_idx, extreme_idx = get_events_per_level(y)
    print(
        f"{suffix} \t {OrdinalPrecipitationLevel.NONE.name}, {OrdinalPrecipitationLevel.WEAK.name}, {OrdinalPrecipitationLevel.STRONG.name}, {OrdinalPrecipitationLevel.MODERATE.name}, {OrdinalPrecipitationLevel.EXTREME.name}:",
        end=" ",
    )
    print(
        f"{y[none_idx].shape[0]}, {y[weak_idx].shape[0]}, {y[moderate_idx].shape[0]}, {y[strong_idx].shape[0]}, {y[extreme_idx].shape[0]}"
    )


def main(argv):
    parser = argparse.ArgumentParser(
        description="""This script builds the train/val/test datasets for a given weather station, 
        by using the user-specified data sources.""",
        prog=sys.argv[0],
    )
    # Add an argument to accept the station of interest
    parser.add_argument(
        "-s",
        "--station_id",
        type=str,
        required=True,
        help="id of the station of interest",
    )
    # Add an argument to accept the pipeline identifier
    parser.add_argument(
        "-p", "--pipeline_id", type=str, required=True, help="id of the pipeline"
    )
    # Add an argument to accept one or more identifiers
    parser.add_argument(
        "-i",
        "--identifiers",
        type=str,
        nargs="+",
        help="IDs of one or more weather stations to merge.",
    )
    parser.add_argument(
        "-o",
        "--only_pos",
        action="store_true",
        help="Boolean argument. If provided, only positive events (i.e., events with target (rainfall) greater than zero) are used in the augmentation.",
    )

    # Parse the arguments
    args = parser.parse_args()

    use_only_pos_examples = args.only_pos

    # Access the list of identifiers
    soi_pipeline_id = args.pipeline_id
    wsoi_id = args.station_id
    identifiers = args.identifiers

    if not (
        (wsoi_id in INMET_WEATHER_STATION_IDS)
        or (wsoi_id in ALERTARIO_WEATHER_STATION_IDS)
    ):
        print(f"Invalid station identifier: {wsoi_id}")
        parser.print_help()
        sys.exit(2)

    #
    # Load numpy arrays (stored in a pickle file) for the WSoI (weather station of interest).
    filename = DATASETS_DIR + soi_pipeline_id + ".pickle"
    print(f"Loading train/val/test datasets from {filename} for WSoI.")
    file = open(filename, "rb")
    (wsoi_X_train, wsoi_y_train, wsoi_X_val, wsoi_y_val, wsoi_X_test, wsoi_y_test) = (
        pickle.load(file)
    )
    print(
        f"Number of examples (train/val/test): {len(wsoi_X_train)}/{len(wsoi_X_val)}/{len(wsoi_X_test)}."
    )
    print(
        f"Min values of train/val/test data matrices: {min(wsoi_X_train.reshape(-1, 1))}/{min(wsoi_X_val.reshape(-1, 1))}/{min(wsoi_X_test.reshape(-1, 1))}"
    )
    print(
        f"Max values in the train/val/test data matrices: {max(wsoi_X_train.reshape(-1, 1))}/{max(wsoi_X_val.reshape(-1, 1))}/{max(wsoi_X_test.reshape(-1, 1))}"
    )
    print_events_by_level(str(wsoi_id) + "/train", wsoi_y_train)
    print_events_by_level(str(wsoi_id) + "/val", wsoi_y_val)
    print_events_by_level(str(wsoi_id) + "/test", wsoi_y_test)
    print()

    augmented_X_train = wsoi_X_train
    augmented_y_train = wsoi_y_train
    augmented_X_val = wsoi_X_val
    augmented_y_val = wsoi_y_val

    if wsoi_id in INMET_WEATHER_STATION_IDS:
        stations_filename = "./data/ws/WeatherStations.csv"
        df_stations = pd.read_csv(stations_filename)
        row = df_stations[df_stations["STATION_ID"] == wsoi_id].iloc[0]
        wsoi_lat_lon = (row["VL_LATITUDE"], row["VL_LONGITUDE"])
    elif wsoi_id in ALERTARIO_WEATHER_STATION_IDS:
        stations_filename = "./data/ws/alertario_stations.parquet"
        df_stations = pd.read_parquet(stations_filename)
        row = df_stations[df_stations["estacao_desc"] == wsoi_id].iloc[0]
        wsoi_lat_lon = (row["latitude"], row["longitude"])

    for ws_id in identifiers:
        print(f"Merging data from weather station {ws_id}...", end="")

        if wsoi_id in INMET_WEATHER_STATION_IDS:
            row = df_stations[df_stations["STATION_ID"] == ws_id].iloc[0]
            ws_lat_lon = (row["VL_LATITUDE"], row["VL_LONGITUDE"])
        elif wsoi_id in ALERTARIO_WEATHER_STATION_IDS:
            row = df_stations[df_stations["estacao_desc"] == ws_id].iloc[0]
            ws_lat_lon = (row["latitude"], row["longitude"])

        dist = haversine_distance(ws_lat_lon, wsoi_lat_lon)
        print(f"dist({wsoi_id}, {ws_id}) = {dist:.2f} Km.")

        pipeline_id = soi_pipeline_id.replace(wsoi_id, ws_id)
        filename = DATASETS_DIR + pipeline_id + ".pickle"
        print(f"Loading train/val/test datasets from {filename}.")
        file = open(filename, "rb")

        (X_train, y_train, X_val, y_val, _, y_test) = pickle.load(file)
        print(f"Number of examples (train/val): {len(X_train)}/{len(X_val)}.")
        print(
            f"Min values of train/val data matrices: {min(X_train.reshape(-1, 1))}/{min(X_val.reshape(-1, 1))})"
        )
        print(
            f"Max values of train/val data matrices: {max(X_train.reshape(-1, 1))}/{max(X_val.reshape(-1, 1))}"
        )
        print_events_by_level(str(ws_id) + "/train", y_train)
        print_events_by_level(str(ws_id) + "/val", y_val)
        print_events_by_level(str(ws_id) + "/test", y_test)

        if use_only_pos_examples:
            temp = len(X_train)
            y_train_gt_zero_idxs = np.where(y_train > 0)[0]
            X_train = X_train[y_train_gt_zero_idxs]
            y_train = y_train[y_train_gt_zero_idxs]

            # y_val_gt_zero_idxs = np.where(y_val > 0)[0]
            # X_val = X_val[y_val_gt_zero_idxs]
            # y_val = y_val[y_val_gt_zero_idxs]

            # augmented_X_train = np.concatenate((augmented_X_train, X_train))
            # augmented_y_train = np.concatenate((augmented_y_train, y_train))
            # augmented_X_val = np.concatenate((augmented_X_val, X_val))
            # augmented_y_val = np.concatenate((augmented_y_val, y_val))

            print(f"Ratio of positive examples: {len(X_train)} of {temp}.")

        augmented_X_train = np.concatenate((augmented_X_train, X_train))
        augmented_y_train = np.concatenate((augmented_y_train, y_train))

        print()

    #
    # Write resulting merged numpy arrays for train/val/test datasets to a single pickle file
    print(
        f"Number of examples in the merged datasets (train/val/test): {len(augmented_X_train)}/{len(augmented_X_val)}/{len(wsoi_X_test)}."
    )
    print_events_by_level("aug/train", augmented_y_train)
    print_events_by_level("aug/val", augmented_y_val)
    print_events_by_level("aug/test", wsoi_y_test)

    merge_list = "_".join(identifiers)
    filename = DATASETS_DIR + soi_pipeline_id + "_" + merge_list + ".pickle"
    print(
        f"Dumping merged train/val/test np arrays to pickle file {filename}.", end=" "
    )
    file = open(filename, "wb")
    ndarrays = (
        augmented_X_train,
        augmented_y_train,
        augmented_X_val,
        augmented_y_val,
        wsoi_X_test,
        wsoi_y_test,
    )
    pickle.dump(ndarrays, file)
    print("Done!")


if __name__ == "__main__":
    main(sys.argv)
