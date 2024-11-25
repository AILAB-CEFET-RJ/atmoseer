import os
import shutil
import logging
from datetime import datetime, timedelta
from netCDF4 import Dataset
import numpy as np
import s3fs
import argparse
import sys

# Define coordinate limits of interest
lon_min, lon_max = -45.05290312102409, -42.35676996062447
lat_min, lat_max = -23.801876626302175, -21.699774257353113

# Directories
output_directory = "data/goes16/glm_files/"
temp_directory = os.path.join(output_directory, "temp")
final_directory = os.path.join(output_directory, "aggregated_data")


def create_directory(directory):
    """Create a directory if it does not exist."""
    os.makedirs(directory, exist_ok=True)
    logging.info(f"Directory {directory} created (or already exists).")


def clear_directory(directory):
    """Clear the contents of a directory and recreate it."""
    if os.path.exists(directory):
        shutil.rmtree(directory)
        logging.info(f"Directory {directory} cleared.")
    create_directory(directory)


def filter_by_coordinates(file_path):
    """Filter GLM events in a NetCDF file based on given coordinates."""
    try:
        dataset = Dataset(file_path, 'r')
        longitudes = dataset.variables['flash_lon'][:]
        latitudes = dataset.variables['flash_lat'][:]

        mask = (
            (longitudes >= lon_min) & (longitudes <= lon_max) &
            (latitudes >= lat_min) & (latitudes <= lat_max)
        )
        dataset.close()

        if np.sum(mask) == 0:
            logging.warning(f"No events found in {file_path} for the specified region. Deleting file.")
            os.remove(file_path)
            return False
        logging.info(f"Events found in {file_path}.")
        return True
    except Exception as e:
        logging.error(f"Error processing file {file_path}: {e}")
        return False


def aggregate_daily_files(day_directory, output_file):
    """Aggregate all filtered files from a day into a single NetCDF file."""
    files = [os.path.join(day_directory, f) for f in os.listdir(day_directory) if f.endswith('.nc')]
    if not files:
        logging.warning(f"No files found in directory {day_directory}.")
        return

    logging.info(f"Aggregating {len(files)} files from {day_directory} into {output_file}.")
    all_longitudes, all_latitudes, all_times = [], [], []

    for file in files:
        try:
            with Dataset(file, 'r') as ds:
                longitudes = ds.variables['flash_lon'][:]
                latitudes = ds.variables['flash_lat'][:]
                times = ds.variables['flash_time_offset_of_first_event'][:]

                all_longitudes.append(longitudes)
                all_latitudes.append(latitudes)
                all_times.append(times)
        except Exception as e:
            logging.error(f"Error reading file {file}: {e}")

    if all_longitudes:
        with Dataset(output_file, 'w', format='NETCDF4') as ds_out:
            ds_out.createDimension('event', len(np.concatenate(all_longitudes)))

            lon_var = ds_out.createVariable('flash_lon', 'f4', ('event',))
            lat_var = ds_out.createVariable('flash_lat', 'f4', ('event',))
            time_var = ds_out.createVariable('flash_time_offset_of_first_event', 'f4', ('event',))

            lon_var[:] = np.concatenate(all_longitudes)
            lat_var[:] = np.concatenate(all_latitudes)
            time_var[:] = np.concatenate(all_times)

        logging.info(f"Aggregated file saved to {output_file}.")


def download_files(start_date, end_date, ignored_months):
    """Download and process GLM files for a specified date range, saving daily files in monthly directories."""
    current_date = start_date
    fs = s3fs.S3FileSystem(anon=True)

    clear_directory(temp_directory)  # Ensure temp_directory is empty
    create_directory(final_directory)

    while current_date <= end_date:
        if current_date.month in ignored_months:
            day = current_date.strftime('%Y_%m_%d')
            logging.info(f"Ignoring data for {day}.")
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

        for hour in range(24):
            hour_path = bucket_path + f"{hour:02d}/"
            try:
                hourly_files = fs.ls(hour_path)
            except FileNotFoundError:
                logging.warning(f"Hour {hour:02d} not found in the bucket. Skipping...")
                continue

            for file in hourly_files:
                file_name = file.split('/')[-1]
                local_file_path = os.path.join(temp_directory, file_name)
                logging.info(f"Downloading: {file} to {local_file_path}")
                fs.get(file, local_file_path)
                filter_by_coordinates(local_file_path)

        # Path for the aggregated file of the day
        aggregated_file = os.path.join(month_dir, f"{day_str}.nc")
        aggregate_daily_files(temp_directory, aggregated_file)

        # Clear the temporary folder for the next day
        clear_directory(temp_directory)

        current_date += timedelta(days=1)


def main(argv):
    parser = argparse.ArgumentParser(description='Download and filter GLM files by coordinates.')
    parser.add_argument('-b', '--start_date', required=True, help='Start date in YYYY-MM-DD format')
    parser.add_argument('-e', '--end_date', required=True, help='End date in YYYY-MM-DD format')
    parser.add_argument('-i', '--ignored_months', nargs='*', type=int, default=[], 
                        help='Months to ignore (e.g., 1 2 12 to ignore January, February, and December)')
    args = parser.parse_args(argv[1:])

    start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
    end_date = datetime.strptime(args.end_date, '%Y-%m-%d')

    assert start_date <= end_date, "Start date must be earlier than or equal to end date."

    ignored_months = set(args.ignored_months)
    for month in ignored_months:
        assert 1 <= month <= 12, f"Invalid month: {month}. Months should be between 1 and 12."

    logging.info(f"Starting download from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    logging.info(f"Ignored months: {', '.join(map(str, ignored_months)) if ignored_months else 'None'}")

    download_files(start_date, end_date, ignored_months)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    main(sys.argv)
