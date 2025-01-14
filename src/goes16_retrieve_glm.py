from concurrent.futures import ThreadPoolExecutor
import logging
import os
import time
from datetime import datetime, timedelta
from netCDF4 import Dataset
import s3fs
import shutil
import numpy as np
import netCDF4 as nc
import numpy.ma as ma
import sys
import argparse
import globals

# Define coordinate limits of interest
lon_min, lon_max = globals.lon_min, globals.lon_max
lat_min, lat_max = globals.lat_min, globals.lat_max

# Directories
output_directory = "data/goes16/GLM/"
temp_directory = os.path.join(output_directory, "temp")
final_directory = os.path.join(output_directory, "aggregated_data")

def create_directory(directory):
    os.makedirs(directory, exist_ok=True)
    logging.info(f"Directory {directory} created (or already exists).")

def clear_directory(directory):
    if os.path.exists(directory):
        shutil.rmtree(directory)
        logging.info(f"Directory {directory} cleared.")
    create_directory(directory)

def create_grid_spatial_resolution(lon_min, lon_max, lat_min, lat_max, flash_lat, flash_lon, spatial_resolution):
    n_lat = round((lat_max - lat_min) / spatial_resolution)
    n_lon = round((lon_max - lon_min) / spatial_resolution)

    shape = (n_lat, n_lon)

    # Interval size
    intervalo_lat = (lat_max - lat_min) / n_lat
    intervalo_lon = (lon_max - lon_min) / n_lon
    # Initialize grid with zeroes
    grid = ma.zeros(shape, dtype=int)
    # Iterating over (lat, lon) list
    for lat, lon in zip(flash_lat, flash_lon):
        if not (lat_min <= lat <= lat_max and lon_min <= lon <= lon_max):
            continue # Skipping (lat, lon) out of the bound
        # Intervals idx
        idx_lat = int((lat - lat_min) / intervalo_lat)
        idx_lon = int((lon - lon_min) / intervalo_lon)
        # Idx on range
        idx_lat = max(0, min(idx_lat, n_lat - 1))
        idx_lon = max(0, min(idx_lon, n_lon - 1))
        # Adding one to the corresponding pair matched on the grid
        grid[idx_lat, idx_lon] += 1

    return grid

def generate_structure(year, month, day):
    # Define the fixed start of the day.
    base_date = datetime(year, month, day, 0, 0)  # yyyy-mm-dd hh:mm
    
    # Creating an empty dictionary
    data = {}
    
    # Generating 48 timestamps starting from 00:00, with intervals of 30 minutes
    for i in range(48):
        timestamp = base_date + timedelta(minutes=30 * i)
        timestamp_key = timestamp.strftime("%Y_%m_%d_%H_%M")  # Format: yyyy_mm_dd_hh_mm
        data[timestamp_key] = {'latitude': [], 'longitude': []}
    
    return data

def adjust_to_previous_interval(dt, interval_minutes=30):

    # Calculate the number of intervals that have passed since the start of the day
    total_minutes = dt.hour * 60 + dt.minute
    previous_interval_minutes = (total_minutes // interval_minutes) * interval_minutes
    adjusted_time = datetime(dt.year, dt.month, dt.day) + timedelta(minutes=previous_interval_minutes)

    return adjusted_time.strftime("%Y_%m_%d_%H_%M")

# Função para download
def download_and_process_file(file, fs, temp_directory):
    local_file_path = os.path.join(temp_directory, os.path.basename(file))
    logging.info(f"Downloading {file} to {local_file_path}")
    fs.get(file, local_file_path)
    logging.info(f"Downloaded: {file}")
    return local_file_path

# Processamento paralelo com threading
def process_hourly_files(fs, hour_path, temp_directory):
    try:
        hourly_files = fs.ls(hour_path)
    except FileNotFoundError:
        logging.warning(f"Hour path {hour_path} not found.")
        return

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(download_and_process_file, file, fs, temp_directory)
            for file in hourly_files
        ]
        for future in futures:
            try:
                file_path = future.result()
                logging.info(f"Processing completed for {file_path}")
            except Exception as e:
                logging.error(f"Error processing file: {e}")

def aggregate_daily_files(day_directory, output_file, year, month, day, spatial_resolution):
    """Aggregate all filtered files from a day into a single NetCDF file."""
    files = [os.path.join(day_directory, f) for f in os.listdir(day_directory) if f.endswith('.nc')]
    if not files:
        logging.warning(f"No files found in directory {day_directory}.")
        return

    logging.info(f"Aggregating {len(files)} files from {day_directory} into {output_file}.")

    # Initialization of a structure to store the latitudes and longitudes for the 48 timestamps
    data = generate_structure(year, month, day)

    for file in files:
        try:
            with Dataset(file, 'r') as ds:
                longitudes = ds.variables['flash_lon'][:]
                latitudes = ds.variables['flash_lat'][:]
                time_coverage_start = ds.getncattr("time_coverage_start")
                dt = datetime.strptime(time_coverage_start, "%Y-%m-%dT%H:%M:%S.%fZ")

                # Adjusting timestamp with the expected format
                formatted_time = adjust_to_previous_interval(dt, interval_minutes=30)

                # Update lat lon  
                data[formatted_time]['latitude'].extend(latitudes)
                data[formatted_time]['longitude'].extend(longitudes)
                
        except Exception as e:
            logging.error(f"Error reading file {file}: {e}")

    # Create a new netCDF file
    with nc.Dataset(output_file, 'w', format='NETCDF4') as dataset:
        # Loop through the dictionary and add data to the netCDF file
        for key in data:  
            latitude = data[key]['latitude']
            longitude = data[key]['longitude']
            shape = (94, 121)
            grid = create_grid_spatial_resolution(lon_min, lon_max, lat_min, lat_max, latitude, longitude, spatial_resolution)

            # Create dimensions based on the shape of the numpy array
            for i, dim_size in enumerate(grid.shape):
                dim_name = f"dim_{i}_{key}"
                if dim_name not in dataset.dimensions:
                    dataset.createDimension(dim_name, dim_size)
            
            # Create a variable with the timestamp as its name
            var = dataset.createVariable(key, grid.dtype, tuple(f"dim_{i}_{key}" for i in range(grid.ndim)))
            
            # Assign the data from the numpy array to the variable
            var[:] = grid  

    logging.info(f"netCDF file '{output_file}' created successfully.")

def download_files(start_date, end_date, ignored_months, spatial_resolution):
    """Download and process GLM files for a specified date range, saving daily files in monthly directories."""
    current_date = start_date
    fs = s3fs.S3FileSystem(anon=True)

    clear_directory(temp_directory) # Ensure temp_directory is empty
    create_directory(final_directory)

    while current_date <= end_date:
        if current_date.month in ignored_months:
            logging.info(f"Ignoring data for {current_date.strftime('%Y-%m-%d')}.")
            current_date += timedelta(days=1)
            continue

        year = current_date.year
        month = current_date.month
        day = current_date.day
        day_str = current_date.strftime('%Y-%m-%d')

        year_dir = os.path.join(final_directory, f"{year}")
        month_dir = os.path.join(year_dir, f"{month:02d}")
        create_directory(month_dir)

        logging.info(f"Processing files for {day_str}.")

        # Path in the S3 bucket for the day
        day_of_year = current_date.timetuple().tm_yday
        bucket_path = f'noaa-goes16/GLM-L2-LCFA/{year}/{day_of_year:03d}/'

        with ThreadPoolExecutor(max_workers=4) as executor:
            for hour in range(24):
                hour_path = bucket_path + f"{hour:02d}/"
                executor.submit(process_hourly_files,fs, hour_path, temp_directory)

        # Path for the aggregated file of the day
        aggregated_file = os.path.join(month_dir, f"{day_str}.nc")
        aggregate_daily_files(temp_directory, aggregated_file, year, month, day, spatial_resolution)

        # Limpeza de diretório temporário
        clear_directory(temp_directory)
        current_date += timedelta(days=1)

def main(argv):
    '''
    Example usage (download data for one specific day):
    python src/goes16_retrieve_glm.py --start_date 2019-01-01 --end_date 2019-01-01 --spatial_resolution 0.1 --ignored_months 6 7 8
    '''

    parser = argparse.ArgumentParser(description='Download and filter GLM files by coordinates.')
    parser.add_argument('-b', '--start_date', required=True, help='Start date in YYYY-MM-DD format')
    parser.add_argument('-e', '--end_date', required=True, help='End date in YYYY-MM-DD format')
    parser.add_argument('-s', '--spatial_resolution', type=float, required=True, help='Spatial resolution in degrees (e.g., 0.1 for 0.1 degrees of latitude/longitude)')
    parser.add_argument('-i', '--ignored_months', nargs='*', type=int, default=[], 
                        help='Months to ignore (e.g., 1 2 12 to ignore January, February, and December)')
    args = parser.parse_args(argv[1:])

    start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
    end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
    spatial_resolution = args.spatial_resolution

    assert start_date <= end_date, "Start date must be earlier than or equal to end date."

    ignored_months = set(args.ignored_months)
    for month in ignored_months:
        assert 1 <= month <= 12, f"Invalid month: {month}. Months should be between 1 and 12."

    logging.info(f"Starting download from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    logging.info(f"Ignored months: {', '.join(map(str, ignored_months)) if ignored_months else 'None'}")

    start_time = time.time()  # Record the start time
    download_files(start_date, end_date, ignored_months, spatial_resolution)
    end_time = time.time()  # Record the end time
    duration = (end_time - start_time) / 60  # Calculate duration in minutes
    print(f"Script execution time: {duration:.2f} minutes.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    main(sys.argv)
