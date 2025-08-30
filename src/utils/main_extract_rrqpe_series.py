from netCDF4 import Dataset                     # Read / Write NetCDF4 files
import matplotlib.pyplot as plt                 # Plotting library
from datetime import timedelta, date, datetime  # Basic Dates and time types
import cartopy, cartopy.crs as ccrs             # Plot maps
import os                                       # Miscellaneous operating system interfaces
from osgeo import gdal                          # Python bindings for GDAL
import numpy as np                              # Scientific computing with Python

from datetime import datetime

import pandas as pd

def dictionary_to_dataframe(input_dict):
    # Create a DataFrame from the dictionary
    df = pd.DataFrame(list(input_dict.items()), columns=['Datetime', 'Value'])
    
    # If you want to set the 'Datetime' column as the index
    df.set_index('Datetime', inplace=True)
    
    # Sort the DataFrame by the 'Datetime' index in ascending order
    df.sort_index(inplace=True)

    return df

def extract_datetime_from_string(input_string):
    try:
        # Split the string by the underscore character to get the date part
        date_str = input_string.split('_')[-1]

        # Remove the file extension (if any) by splitting at the last dot ('.') and taking the first part
        date_str = date_str.split('.')[0]

        # Use strptime to parse the date string into a datetime object
        # Adjust the format to match the date format in your string (YYYYMMDDHHMM)
        date_format = "%Y%m%d%H%M"
        extracted_datetime = datetime.strptime(date_str, date_format)
        return extracted_datetime
    except ValueError:
        # Handle parsing errors if the format doesn't match
        return None


def get_rrqpe_value(filename):
    #  Read file netcdf with estimated rain from GOES-16
    sat_data = gdal.Open(filename)

    # Read number of cols and rows
    ncol = sat_data.RasterXSize
    nrow = sat_data.RasterYSize
    print(f'ncol, nrow = {ncol}, {nrow}')

    # Load the data
    sat_array = sat_data.ReadAsArray(0, 0, ncol, nrow).astype(float)

    # Get geotransform
    transform = sat_data.GetGeoTransform()

    # Coordenadas da estação do Forte de Copacabana
    lat = -22.98833333
    lon = -43.19055555

    x = int((lon - transform[0]) / transform[1])
    y = int((transform[3] - lat) / -transform[5])

    # print(f'Value at ({x},{y}): {sat_array[x,y]}')

    return sat_array[x,y]
    


def get_rrqpe_series(folder_path):
    try:
        # Use os.listdir to get a list of all files and directories in the folder
        files = os.listdir(folder_path)

        observations = dict()

        # Iterate through the list of files
        for file in files:
            if os.path.isfile(os.path.join(folder_path, file)):
                timestamp = extract_datetime_from_string(file)
                assert timestamp is not None
                full_filename = os.path.join(folder_path, file)
                observations[timestamp] = get_rrqpe_value(full_filename)
        
        return observations

    except FileNotFoundError:
        print(f"The folder '{folder_path}' does not exist.")
    except PermissionError:
        print(f"Permission denied to access folder '{folder_path}'.")

if __name__ == "__main__":
    folder_path = './data/goes16/Output'
    print(folder_path)
    series = get_rrqpe_series(folder_path)
    df = dictionary_to_dataframe(series)
    print(f'Extracted series has {df.shape[0]} points, from {min(df.index)} to {max(df.index)}.')
    print(df.head())
    df.to_csv('./data/goes16/rrqpe.csv')