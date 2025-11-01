import argparse
import glob
import os
import sys

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import xarray as xr

# from src.utils.util import *

station_ids_for_goes16 = {
    "A652": {"latitude": -22.98833333, "longitude": -43.19055555},
    "A636": {"latitude": -22.93999999, "longitude": -43.40277777},
    "A621": {"latitude": -22.86138888, "longitude": -43.41138888},
    "A602": {"latitude": -23.05027777, "longitude": -43.59555555},
    "A627": {"latitude": -22.86749999, "longitude": -43.10194444},
}


def haversine(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points on the earth.

    This function computes the distance using the Haversine formula, which is an equation giving the shortest distance between any two points on the surface of a sphere.

    Parameters:
    lat1 (float): Latitude of the first point in decimal degrees.
    lon1 (float): Longitude of the first point in decimal degrees.
    lat2 (float): Latitude of the second point in decimal degrees.
    lon2 (float): Longitude of the second point in decimal degrees.

    Returns:
    float: Distance between the two points in kilometers.

    Note:
    The radius of the Earth is assumed to be 6371 kilometers.
    """
    R = 6371  # Earth radius in kilometers
    dLat = np.radians(lat2 - lat1)
    dLon = np.radians(lon2 - lon1)
    a = np.sin(dLat / 2) * np.sin(dLat / 2) + np.cos(np.radians(lat1)) * np.cos(
        np.radians(lat2)
    ) * np.sin(dLon / 2) * np.sin(dLon / 2)
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    distance = R * c
    return distance


def read_and_process_files(files, station_id, g16_pre_process_data_file):
    """
    Read and process a batch of NetCDF files containing TPW data.

    Args:
        files (list of str): A list of file paths to NetCDF files.
        station_id (str): The station ID for processing data.

    Returns:
        g16_pre_process_data_file (list): list with file pre processed
    """
    g16_data_file = []
    station_lat = station_ids_for_goes16[station_id]["latitude"]
    station_lon = station_ids_for_goes16[station_id]["longitude"]
    # Define a radius (in kilometers) for filtering
    radius_km = 10
    for i in range(0, len(files)):
        try:
            nc_indx = i
            g16_data = files[nc_indx]
            g16_data_file.append(g16_data)
            ds = xr.open_dataset(
                g16_data_file[i],
                cache=False,
            )
            df = ds.to_dataframe()
            df = df[
                df.apply(
                    lambda row: haversine(
                        station_lat, station_lon, row["event_lat"], row["event_lon"]
                    )
                    <= radius_km,
                    axis=1,
                )
            ]
            df["event_time_offset"] = df["event_time_offset"].astype("datetime64[us]")
            g16_pre_process_data_file["Datetime"].extend(df["event_time_offset"])
            g16_pre_process_data_file["event_energy"].extend(df["event_energy"])
            ds.close()
        except:
            pass

    return g16_pre_process_data_file


def pre_process_tpw_product(path, station_id):
    """
    Preprocess Total Precipitable Water (TPW) data from NetCDF files and save it to a Parquet file.

    Args:
        path (str): The path to the directory containing NetCDF data files.
        station_id (str): The station ID for processing data.

    Returns:
        None

    This function reads TPW data from a batch of NetCDF files, processes it, and saves it to a Parquet file.
    The function navigates to the specified directory, collects TPW files, and processes them in batches of
    1000 files to optimize memory usage. Processed data is stored in a Pandas DataFrame and then appended
    to an existing Parquet file or a new one is created if it doesn't exist. Finally, the function returns
    None after completing the preprocessing and saving.
    """
    # navigate to directory with .nc data files
    os.chdir(str(path))
    nc_files = glob.glob("*GLM-L2-LCFA*")
    nc_files = sorted(nc_files)

    parquet_dir = "data/parquet_files"

    if not os.path.exists(parquet_dir):
        os.makedirs(parquet_dir)

    parquet_path = f"data/parquet_files/glm_{station_id}_preprocessed_file.parquet"

    batch_size = 1000
    total_files = len(nc_files)

    g16_pre_process_data_file = {"Datetime": [], "event_energy": []}

    print(f"You have {total_files} to be processed")

    for i in range(0, total_files, batch_size):
        batch_files = nc_files[i : i + batch_size]
        read_and_process_files(batch_files, station_id, g16_pre_process_data_file)
        print(f"{len(g16_pre_process_data_file)} Files was pre processed")

    # Create a DataFrame from the list of dictionaries
    df = pd.DataFrame(g16_pre_process_data_file)

    # Set the 'Datetime' column as the DatetimeIndex
    df["Datetime"] = pd.to_datetime(df["Datetime"])
    df = df.set_index(pd.DatetimeIndex(df["Datetime"]))

    # Remove time-related columns since now this information is in the index.
    df = df.drop(["Datetime"], axis=1)

    # Append to the existing Parquet file or create a new one
    if os.path.exists(parquet_path):
        table = pq.read_table(parquet_path)
        df_existing = table.to_pandas()
        df_combined = pd.concat([df_existing, df])
    else:
        df_combined = df

    # Save the combined DataFrame to a Parquet file
    df_combined.to_parquet(parquet_path, compression="gzip")

    return


def main(argv):
    parser = argparse.ArgumentParser(
        description="Preprocess ABI products station data."
    )
    parser.add_argument(
        "-s",
        "--station_id",
        required=True,
        help="ID of the weather station to preprocess data for.",
    )
    args = parser.parse_args(argv[1:])

    directory = "data/goes16/glm_files"

    station_id = args.station_id

    print("\n***Preprocessing GLM Files***")
    pre_process_tpw_product(directory, station_id)
    print("Done!")


if __name__ == "__main__":
    main(sys.argv)
