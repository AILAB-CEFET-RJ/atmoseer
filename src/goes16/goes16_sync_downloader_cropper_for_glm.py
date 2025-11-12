import s3fs
import os
import time
from datetime import datetime, timedelta
from netCDF4 import Dataset
import netCDF4 as nc
import threading
from concurrent.futures import ThreadPoolExecutor
import argparse
import logging
# import netCDF4 as nc
import numpy as np
import sys

# Lock to synchronize access to shared resources
download_lock = threading.Lock()

# Thread-safe dict to keep track of cropped content
cropped_dict = {}
cropped_dict_lock = threading.Lock()  # Initialize the lock

########################################################################
### DOWNLOADER
########################################################################

# Output directories
output_directory = "data/goes16/glm_files/"
temp_directory = "data/goes16/temp_glm_files/"
final_directory = "data/goes16/aggregated_glm_files/"

# print(f"Processing {remote_path}")
lat_max, lon_max = (
            -21.699774257353113,
            -42.35676996062447,
)  # canto superior direito
lat_min, lon_min = (
            -23.801876626302175,
            -45.05290312102409,
)  # canto inferior esquerdo
extent = [lon_min, lat_min, lon_max, lat_max]

# Full disk download and crop function
def download_and_crop_full_disk(fs, remote_path: str, local_path: str):
    try:
        print(f'Downloading {remote_path} to {local_path}')
        fs.get(remote_path, local_path)
        # print(f"Processing {remote_path}")
        cropped_content = crop_full_disk(file_path = local_path, extent = extent)

        # Lock to ensure thread-safe access
        with cropped_dict_lock:
            # print('Updating cropped_dict')
            cropped_dict.update(cropped_content)  # Add the cropped content in a thread-safe way

        os.remove(local_path)  # Delete the local copy of the FD file after processing
    except Exception as e:
        print(f"Error downloading {os.path.basename(remote_path)}: {e}")

# Function to convert a regular date to the Julian day of the year
def get_julian_day(date):
    return date.strftime('%j')

# Function to download files from GOES-16 for a specific hour
def process_goes16_data_for_hour(fs, s3_path, download_dir):
    # List all files in the given hour directory
    try:
        files = fs.ls(s3_path)
    except Exception as e:
        print(f"Error accessing S3 path {s3_path}: {e}")
        return

    # print(f'# files in {s3_path}: {len(files)}')

    # Download each file using threads
    with ThreadPoolExecutor() as executor:
        for remote_path in files:
            file_name = os.path.basename(remote_path)
            local_path = os.path.join(download_dir, file_name)
            if not os.path.exists(local_path):
                executor.submit(download_and_crop_full_disk, fs, remote_path, local_path)

# Function to download all files for a given day
def process_goes16_data_for_day(date, download_dir):
    # Set up S3 access
    fs = s3fs.S3FileSystem(anon=True)
    bucket = 'noaa-goes16'
    product = f'GLM-L2-LCFA'

    # Format the date
    year = date.strftime('%Y')
    julian_day = get_julian_day(date)

    # Download data for each hour concurrently
    with download_lock:
        with ThreadPoolExecutor() as executor:
            for hour in range(24):
                hour_str = f'{hour:02d}'
                s3_path = f'{bucket}/{product}/{year}/{julian_day}/{hour_str}/'
                # Submit each hour's download process to the thread pool
                executor.submit(process_goes16_data_for_hour, fs, s3_path, download_dir)

# Main function to process files for a range of dates
def process_goes16_data_for_period(start_date, end_date, ignored_months, download_dir, crop_dir):
    current_date = start_date
    while current_date <= end_date:
        day = current_date.strftime('%Y_%m_%d')
        if (current_date.month in ignored_months) or any(day in filename for filename in os.listdir(crop_dir)):
                print(f"Ignoring data for {day}")
                current_date += timedelta(days=1)
                continue

        print(f"Processing data for {day}")
        process_goes16_data_for_day(current_date, download_dir)

        netcdf_filename = f'{crop_dir}/GLM_{day}.nc'
        global cropped_dict
        save_to_netcdf(cropped_dict, netcdf_filename)
        cropped_dict = {}

        current_date += timedelta(days=1)

########################################################################
### CROPPER
########################################################################

def extract_middle_part(file_path):
    # Split the file path by '/' and get the last part (the filename)
    filename = file_path.split('/')[-1]
    
    # The middle part ends right before the '_s' section
    middle_part = filename.split('_s')[0]
    
    return middle_part

def crop_full_disk(file_path, extent):
    """
    Filter GLM events in a NetCDF file based on the provided coordinate bounds.

    Parameters:
        file_path (str): Path to the input NetCDF file.
        lon_min (float): Minimum longitude of the bounding box.
        lon_max (float): Maximum longitude of the bounding box.
        lat_min (float): Minimum latitude of the bounding box.
        lat_max (float): Maximum latitude of the bounding box.
    
    Returns:
        Dataset: A new in-memory NetCDF Dataset object containing filtered data.
    """
    try:
        print("0 --> ", file_path)

        lon_min, lat_min, lon_max, lat_max = extent
        
        # sys.exit(0)

        # Open the original NetCDF file in read mode
        dataset = Dataset(file_path, 'r')
        
        print("1 --> Opened successfully!")

        # Get the longitude and latitude variables
        longitudes = dataset.variables['flash_lon'][:]
        latitudes = dataset.variables['flash_lat'][:]
        
        # Create a mask for the bounding box
        mask = (
            (longitudes >= lon_min) & (longitudes <= lon_max) &
            (latitudes >= lat_min) & (latitudes <= lat_max)
        )

        # Create an in-memory NetCDF dataset for filtered data
        filtered_dataset = Dataset('filtered_data.nc', 'w', format='NETCDF4', memory=True)
        
        # Copy global attributes
        filtered_dataset.setncatts({attr: dataset.getncattr(attr) for attr in dataset.ncattrs()})
        
        # Copy dimensions
        for dim_name, dim in dataset.dimensions.items():
            filtered_dataset.createDimension(dim_name, len(dim) if not dim.isunlimited() else None)
        
        # Copy variables and apply the mask
        for var_name, var in dataset.variables.items():
            # Create the new variable in the filtered dataset
            filtered_var = filtered_dataset.createVariable(var_name, var.datatype, var.dimensions)
            
            # Copy variable attributes
            filtered_var.setncatts({attr: var.getncattr(attr) for attr in var.ncattrs()})
            
            # Apply the mask if the variable is related to flash events
            if var_name in ['flash_lon', 'flash_lat']:
                filtered_var[:] = var[:][mask]
            elif 'event_id' in var.dimensions or 'flash_id' in var.dimensions:
                filtered_var[:] = np.compress(mask, var[:], axis=0)
            else:
                filtered_var[:] = var[:]
        
        # Close the original dataset
        dataset.close()
        
        # Return the filtered dataset
        return filtered_dataset
    
    except Exception as e:
        print(f"An error occurred: {e}")
        return None


def save_to_netcdf(cropped_dict, filename):
    """
    Creates a netCDF file from a dictionary where the keys are date strings
    and the values are numpy arrays.
    
    Args:
    cropped_dict (dict): Dictionary with keys in the format '%Y_%m_%d_%H_%M'
                         and numpy arrays as values.
    filename (str): Path and name of the netCDF file to be created.
    """

    print(f'# cropped files: {len(cropped_dict)}')

    # Create a new netCDF file
    with nc.Dataset(filename, 'w', format='NETCDF4') as dataset:
        # Loop through the dictionary and add data to the netCDF file
        for timestamp, data_array in cropped_dict.items():
            # Replace ':' and spaces with underscores in timestamp to create valid variable names
            sanitized_name = timestamp.replace(":", "_").replace(" ", "_")
            
            # Create dimensions based on the shape of the numpy array
            for i, dim_size in enumerate(data_array.shape):
                dim_name = f"dim_{i}_{sanitized_name}"
                if dim_name not in dataset.dimensions:
                    dataset.createDimension(dim_name, dim_size)
            
            # Create a variable with the timestamp as its name
            var = dataset.createVariable(sanitized_name, data_array.dtype, tuple(f"dim_{i}_{sanitized_name}" for i in range(data_array.ndim)))
            
            # Assign the data from the numpy array to the variable
            var[:] = data_array
            
        print(f"netCDF file '{filename}' created successfully.")


########################################################################
### MAIN
########################################################################

if __name__ == "__main__":
    '''
    Example usage (download data for one specific day):
    python src/goes16_sync_downloader_cropper_for_glm.py --start_date "2024-02-08" --end_date "2024-02-08" --download_dir "./downloads" --crop_dir "./cropped"
    '''

    # Create an argument parser to accept start and end dates, and channel number from the command line
    parser = argparse.ArgumentParser(description="Download GOES-16 data for a specific date range.")
    parser.add_argument('--start_date', type=str, required=True, help="Start date (format: YYYY-MM-DD)")
    parser.add_argument('--end_date', type=str, required=True, help="End date (format: YYYY-MM-DD)")
    parser.add_argument('--download_dir', type=str, default='./downloads', help="Directory to (temporarily) save downloaded FD files")
    parser.add_argument('--crop_dir', type=str, required=True, help="Directory to save cropped files")
    parser.add_argument("--ignored_months", nargs='+', type=int, required=False, default=[6, 7, 8],
                        help="Months to ignore (e.g., --ignored_months 6 7 8)")
    # parser.add_argument("--vars", nargs='+', type=str, required=True, help="At least one variable name (CMI, ...)")

    fmt = "[%(levelname)s] %(funcName)s():%(lineno)i: %(message)s"
    logging.basicConfig(level=logging.INFO, format=fmt)

    args = parser.parse_args()

    # Parse the start and end dates
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
    download_dir = args.download_dir
    crop_dir = args.crop_dir
    ignored_months = args.ignored_months

    start_time = time.time()  # Record the start time
    download_dir = './downloads'
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
    process_goes16_data_for_period(start_date, 
                                   end_date, 
                                   ignored_months, 
                                   download_dir = download_dir, 
                                   crop_dir = crop_dir)
    end_time = time.time()  # Record the end time
    duration = (end_time - start_time) / 60  # Calculate duration in minutes
    print(f"Script execution time: {duration:.2f} minutes.")