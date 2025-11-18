from netCDF4 import Dataset                     # Read / Write NetCDF4 files
import matplotlib.pyplot as plt                 # Plotting library
from datetime import timedelta, date, datetime  # Basic Dates and time types
import cartopy, cartopy.crs as ccrs             # Plot maps
import os                                       # Miscellaneous operating system interfaces
import numpy as np                              # Scientific computing with Python
from matplotlib import cm                       # Colormap handling utilities
from goes16_utils import download_PROD             # Our function for download
from goes16_utils import reproject                 # Our function for reproject
from goes16.processing_data import find_pixel_of_coordinate
from goes16.processing_data import open_dataset
import pandas as pd
import sys
import argparse

from config.globals import INMET_WEATHER_STATION_IDS

from typing import List
from collections import OrderedDict
import time

#------------------------------------------------------------------------------
def download_data_for_a_day(df: pd.DataFrame, 
                            yyyymmdd: str, 
                            stations_of_interest: dict, 
                            product_name: str, 
                            variable_names: List[str], 
                            temporal_resolution: int, 
                            remove_full_disk_file: bool = True):
    """
    Downloads values of a specific variable from a specific product and for a specific day from GOES-16 satellite.
    These values are downloaded only for the locations (lat/lon) of a list of stations of interest. 
    These downloaded values are appended (as new rows) to the provided DataFrame. 
    Each row will have the following columns: (timestamp, station_id, variable names's value)

    Args:
    - df (pandas.DataFrame): DataFrame to which downloaded values for stations of interest will be appended as a new column.
    - yyyymmdd (str): Date in 'YYYYMMDD' format specifying the day for which data will be downloaded.
    - stations_of_interest (dict): Dictionary containing stations of interest with their IDs as keys
                                   and their corresponding latitude and longitude coordinates as values.
    - product_name (str): The name of the GOES-16 product from which data will be downloaded.
    - variable_name (str): The specific variable to be retrieved from the product.

    Returns:
    - pandas.DataFrame: Updated DataFrame with appended TPW values for stations of interest.
    """

    # Directory to temporarily store each downloaded full disk file.
    temp_dir  = "./data/goes16/temp"

    # Initial time and date
    yyyy = datetime.strptime(yyyymmdd, '%Y%m%d').strftime('%Y')
    mm = datetime.strptime(yyyymmdd, '%Y%m%d').strftime('%m')
    dd = datetime.strptime(yyyymmdd, '%Y%m%d').strftime('%d')

    hour_ini = 0
    date_ini = datetime(int(yyyy),int(mm),int(dd),hour_ini,0)
    date_end = datetime(int(yyyy),int(mm),int(dd),hour_ini,0) + timedelta(hours=23)

    time_step = date_ini
  
    #-----------------------------------------------------------------------------------------------------------
    # Accumulation loop. Scans all of the files for the given day. 
    # For each file, gets the TPW values for the locations where 
    # the stations of interested are located.
    while (time_step <= date_end):
        # Date structure
        yyyymmddhhmn = datetime.strptime(str(time_step), '%Y-%m-%d %H:%M:%S').strftime('%Y%m%d%H%M')

        print(f'-Getting data for {yyyymmddhhmn}...')

        # Download the full disk file from the Amazon cloud.
        file_name = download_PROD(yyyymmddhhmn, product_name, temp_dir)

        try:
            full_disk_filename = f'{temp_dir}/{file_name}.nc'
            ds = open_dataset(full_disk_filename)

            if ds is not None:
                for wsoi_id in stations_of_interest:
                    values_dict = OrderedDict()
                    for variable_name in variable_names:
                        field, LonCen, LatCen = ds.image(variable_name, lonlat='center')
                        lon = stations_of_interest[wsoi_id][1]
                        lat = stations_of_interest[wsoi_id][0]
                        x, y = find_pixel_of_coordinate(LonCen, LatCen, lon, lat)
                        value = field.data[y,x]
                        values_dict[variable_name] = value
                    new_row = {'timestamp': yyyymmddhhmn, 'station_id': wsoi_id}
                    new_row.update(values_dict)
                    df = df.append(new_row, ignore_index=True)

                if remove_full_disk_file:
                    try:
                        os.remove(full_disk_filename)  # Use os.remove() to delete the file
                    except FileNotFoundError:
                        print(f"Error: File '{full_disk_filename}' not found.")
                    except PermissionError:
                        print(f"Error: Permission denied to remove file '{full_disk_filename}'.")
                    except Exception as e:
                        print(f"An error occurred: {e}")
        except FileNotFoundError:
            print(f"Error: File '{full_disk_filename}' not found.")

        # Increment to get the next full disk observation.
        time_step = time_step + timedelta(minutes=temporal_resolution)

    return df

def main(argv):
    # Create an argument parser
    parser = argparse.ArgumentParser(description="Retrieve GOES16's data for (user-provided) product, variable, and date range.")
    
    # Add command line arguments for date_ini and date_end
    parser.add_argument("--date_ini", type=str, required=True, help="Start date (format: YYYY-MM-DD)")
    parser.add_argument("--date_end", type=str, required=True, help="End date (format: YYYY-MM-DD)")
    parser.add_argument("--prod", type=str, required=True, help="GOES16 product name (e.g., 'ABI-L2-TPWF', 'ABI-L2-DSIF')")
    parser.add_argument("--vars", nargs='+', type=str, required=True, help="Variable names (e.g., CAPE, CIN, ...)")
    parser.add_argument("--temporal_resolution", type=int, default=10, help="Temporal resolution of the observations, in minutes (default: 10)")
    
    args = parser.parse_args()
    start_date = args.date_ini
    end_date = args.date_end
    product_name = args.prod
    variable_names = args.vars
    temporal_resolution = args.temporal_resolution

    # try:
    #     if (station_id in globals.ALERTARIO_WEATHER_STATION_IDS or station_id in globals.ALERTARIO_GAUGE_STATION_IDS):
    #         # This UTC thing is really anonying!
    #         start_date = pd.to_datetime(args.date_ini, utc=True)
    #         end_date = pd.to_datetime(args.date_end, utc=True)
    #     else:
    #         start_date = pd.to_datetime(args.date_ini)
    #         end_date = pd.to_datetime(args.date_end)
    # except ParserError:
    #     print(f"Invalid date format: {args.date_ini}, {args.date_end}.")
    #     parser.print_help()
    #     sys.exit(2)

    # Load Station Data
    stations_of_interest = dict()
    stations_filename = "./data/ws/WeatherStations.csv"
    df_stations = pd.read_csv(stations_filename)
    for wsoi_id in INMET_WEATHER_STATION_IDS:
        row = df_stations[df_stations["STATION_ID"] == wsoi_id].iloc[0]
        wsoi_lat_lon = (row["VL_LATITUDE"], row["VL_LONGITUDE"])
        stations_of_interest[row["STATION_ID"]] = wsoi_lat_lon

    # Create an empty DataFrame to store historical product values for each station of interest.
    df = pd.DataFrame(columns=['timestamp', 'station_id', *variable_names])

    # Convert start_date and end_date to datetime objects
    from datetime import datetime
    start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
    end_datetime = datetime.strptime(end_date, '%Y-%m-%d')

    # Iterate through the range of user-provided days, 
    # one day at a time, to retrieve corresponding data.
    current_datetime = start_datetime
    while current_datetime <= end_datetime:
        yyyymmdd = current_datetime.strftime('%Y%m%d')
        df = download_data_for_a_day(df, yyyymmdd, stations_of_interest, product_name, variable_names, temporal_resolution=temporal_resolution)
        # Increment the current date by one day
        current_datetime += timedelta(days=1)

    filename = f'{product_name}_{start_datetime}_to_{end_datetime}.parquet'
    filename = filename.replace(' 00:00:00', '')
    df.to_parquet(filename)
    print(f'A Pandas dataframe with shape {df.shape} was created and saved in the file {filename}.')

if __name__ == "__main__":
    ### Examples:
    # python src/retrieve_goes16_product_for_wsois.py --date_ini "2024-03-09" --date_end "2024-03-09" --prod ABI-L2-DSIF --vars CAPE TT CIN
    # python src/retrieve_goes16_product_for_wsois.py --date_ini "2024-01-12" --date_end "2024-01-12" --prod ABI-L2-DSIF --vars CAPE K
    # python src/retrieve_goes16_product_for_wsois.py --date_ini "2024-01-12" --date_end "2024-01-12" --prod ABI-L2-TPWF --vars TPW

    start_time = time.time()  # Record the start time

    main(sys.argv)
    
    end_time = time.time()  # Record the end time
    duration = (end_time - start_time) / 60  # Calculate duration in minutes
    
    print(f"Script duration: {duration:.2f} minutes")    
