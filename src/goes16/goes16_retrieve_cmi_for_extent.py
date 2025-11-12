# Download data from aws
#-----------------------------------------------------------------------------------------------------------
# Required modules
import os                                # Miscellaneous operating system interfaces
import numpy as np                       # Import the Numpy package
from botocore import UNSIGNED            # boto3 config
from botocore.config import Config       # boto3 config                       
from datetime import timedelta, datetime           # Basic Dates and time types
from osgeo import osr                    # Python bindings for GDAL
from osgeo import gdal                   # Python bindings for GDAL
import sys
import argparse
from typing import List
import time
import logging
from netCDF4 import Dataset 
from goes16_utils import download_CMI


def crop_full_disk_and_save(full_disk_filename, variable_names, extent, dest_path, band):
    file = Dataset(full_disk_filename)
    date = (datetime.strptime(file.time_coverage_start, '%Y-%m-%dT%H:%M:%S.%fZ'))
    # print('date1: ', date.strftime('%Y_%m_%d_%H_%M'))
    # print('date2: ', yyyymmddhhmn)

    yyyymmddhhmn = date.strftime('%Y_%m_%d_%H_%M')

    for var in variable_names:
   
        # Open the file
        img = gdal.Open(f'NETCDF:{full_disk_filename}:' + var)

        # Read the header metadata
        metadata = img.GetMetadata()
        scale = float(metadata.get(var + '#scale_factor'))
        offset = float(metadata.get(var + '#add_offset'))
        undef = float(metadata.get(var + '#_FillValue'))
        # dtime = metadata.get('NC_GLOBAL#time_coverage_start')

        # Load the data
        ds = img.ReadAsArray(0, 0, img.RasterXSize, img.RasterYSize).astype(float)

        # Apply the scale and offset
        ds = (ds * scale + offset)

        # Read the original file projection and configure the output projection
        source_prj = osr.SpatialReference()
        source_prj.ImportFromProj4(img.GetProjectionRef())

        target_prj = osr.SpatialReference()
        target_prj.ImportFromProj4("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs")

        # Reproject the data
        GeoT = img.GetGeoTransform()
        driver = gdal.GetDriverByName('MEM')
        raw = driver.Create('raw', ds.shape[0], ds.shape[1], 1, gdal.GDT_Float32)
        raw.SetGeoTransform(GeoT)
        raw.GetRasterBand(1).WriteArray(ds)

        # Define the parameters of the output file  
        options = gdal.WarpOptions(format = 'netCDF', 
                srcSRS = source_prj, 
                dstSRS = target_prj,
                outputBounds = (extent[0], extent[3], extent[2], extent[1]), 
                outputBoundsSRS = target_prj, 
                outputType = gdal.GDT_Float32, 
                srcNodata = undef, 
                dstNodata = 'nan', 
                resampleAlg = gdal.GRA_NearestNeighbour)
        
        img = None  # Close file

        # Write the reprojected file on disk
        filename_reprojected = f'{dest_path}/{var}_band{band}_{yyyymmddhhmn}.nc'
        gdal.Warp(filename_reprojected, raw, options=options)


def download_data_for_a_day(extent: List[float], 
                            dest_path: str,
                            yyyymmdd: str, 
                            variable_names: List[str], 
                            bands:List[str],
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
    """

    # Directory to temporarily store each downloaded full disk file.
    TEMP_DIR  = "data/goes16"

    # Initial time and date
    yyyy = datetime.strptime(yyyymmdd, '%Y%m%d').strftime('%Y')
    mm = datetime.strptime(yyyymmdd, '%Y%m%d').strftime('%m')
    dd = datetime.strptime(yyyymmdd, '%Y%m%d').strftime('%d')

    hour_ini = 0
    date_ini = datetime(int(yyyy),int(mm),int(dd),hour_ini,0)
    date_end = datetime(int(yyyy),int(mm),int(dd),hour_ini,0) + timedelta(hours=23)

    time_step = date_ini
  
    while (time_step <= date_end):
        # Date structure
        yyyymmddhhmn = datetime.strptime(str(time_step), '%Y-%m-%d %H:%M:%S').strftime('%Y%m%d%H%M')

        logging.info(f'-Getting data for {yyyymmddhhmn}...')

        for band in bands:

            # Download the full disk file from the Amazon cloud.
            file_name = download_CMI(yyyymmddhhmn, band, TEMP_DIR)

            if file_name != -1:
                try:
                    full_disk_filename = f'{TEMP_DIR}/{file_name}.nc'

                    crop_full_disk_and_save(full_disk_filename, yyyymmddhhmn, variable_names, extent, dest_path, band)

                    if remove_full_disk_file:
                        try:
                            os.remove(full_disk_filename)  # Use os.remove() to delete the file
                        except FileNotFoundError:
                            logging.info(f"Error: File '{full_disk_filename}' not found.")
                        except PermissionError:
                            logging.info(f"Error: Permission denied to remove file '{full_disk_filename}'.")
                        except Exception as e:
                            logging.info(f"An error occurred: {e}")
                except Exception as e:
                    logging.info(f"An error occurred: {e}")

        # Increment to get the next full disk observation.
        time_step = time_step + timedelta(minutes=temporal_resolution)

def main(argv):
    # Create an argument parser
    parser = argparse.ArgumentParser(description="Retrieve GOES16's data for (user-provided) band, variable, and date range.")
    
    # Add command line arguments for date_ini and date_end
    parser.add_argument("--date_ini", type=str, required=True, help="Start date (format: YYYY-MM-DD)")
    parser.add_argument("--date_end", type=str, required=True, help="End date (format: YYYY-MM-DD)")
    parser.add_argument("--vars", nargs='+', type=str, required=True, help="At least one variable name (CMI, ...)")
    parser.add_argument("--bands", nargs='+', type=str, required=True, help="At least one channel from goes 16")
    parser.add_argument("--temporal_resolution", type=int, default=10, help="Temporal resolution of the observations, in minutes (default: 10)")
  
    # TODO - change to cmd line args
    lat_max, lon_max = (
        -21.699774257353113,
        -42.35676996062447,
    )  # canto superior direito
    lat_min, lon_min = (
        -23.801876626302175,
        -45.05290312102409,
    )  # canto inferior esquerdo

    extent = [lon_min, lat_min, lon_max, lat_max]


    dest_path = "data/goes16"

    args = parser.parse_args()
    start_date = args.date_ini
    end_date = args.date_end
    variable_names = args.vars
    temporal_resolution = args.temporal_resolution
    bands = args.bands

    # Convert start_date and end_date to datetime objects
    from datetime import datetime
    start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
    end_datetime = datetime.strptime(end_date, '%Y-%m-%d')

    folder_path = f'data/goes16'

    # Verifica se a pasta existe
    if not os.path.exists(folder_path):
        # Se não existir, cria a pasta
        os.makedirs(folder_path)
        print(f'Pasta criada: {folder_path}')
    else:
        print(f'A pasta já existe: {folder_path}')

    # Iterate through the range of user-provided days, 
    # one day at a time, to retrieve corresponding data.
    current_datetime = start_datetime
    while current_datetime <= end_datetime:
        # Ignore winter months
        if current_datetime.month not in [6, 7, 8]:
            yyyymmdd = current_datetime.strftime('%Y%m%d')
            df = download_data_for_a_day(extent, dest_path, yyyymmdd,variable_names, bands, temporal_resolution=temporal_resolution)
        # Increment the current date by one day
        current_datetime += timedelta(days=1)


if __name__ == "__main__":
    ### Examples:
    # python src/retrieve_goes16_cmi_for_extent.py --date_ini "2024-02-08" --date_end "2024-02-08" --vars CMI --bands 7 9 11 13 14 15

    fmt = "[%(levelname)s] %(funcName)s():%(lineno)i: %(message)s"
    logging.basicConfig(level=logging.INFO, format = fmt)

    start_time = time.time()  # Record the start time

    main(sys.argv)
    
    end_time = time.time()  # Record the end time
    duration = (end_time - start_time) / 60  # Calculate duration in minutes
    
    print(f"Script execution time: {duration:.2f} minutes.")