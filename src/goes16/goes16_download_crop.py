import s3fs
import os
import time
from datetime import datetime, timedelta
import threading
from concurrent.futures import ThreadPoolExecutor
from osgeo import osr, gdal
import argparse
import logging
import netCDF4 as nc

from config import globals

# Lock to synchronize access to shared resources
download_lock = threading.Lock()

# Thread-safe dict to keep track of cropped content
cropped_dict = {}
cropped_dict_lock = threading.Lock()  # Initialize the lock

########################################################################
### DOWNLOADER
########################################################################

# Full disk download and crop function
def download_and_crop_full_disk(fs, remote_path: str, local_path: str, spatial_resolution: float, variable_names: str):
    try:
        fs.get(remote_path, local_path)
        # print(f"Processing {remote_path}")
        lat_max, lon_max = (
            globals.lat_max,
            globals.lon_max,
        )  # canto superior direito
        lat_min, lon_min = (
            globals.lat_min,
            globals.lon_min,
        )  # canto inferior esquerdo
        extent = [lon_min, lat_min, lon_max, lat_max]
        cropped_content = crop_full_disk(full_disk_filename = local_path,
                                        spatial_resolution = spatial_resolution,
                                         variable_names = variable_names,
                                         extent = extent)

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
def process_goes16_data_for_hour(fs, s3_path, channel, download_dir, spatial_resolution, variable_names):
    # List all files in the given hour directory
    try:
        files = fs.ls(s3_path)
    except Exception as e:
        print(f"Error accessing S3 path {s3_path}: {e}")
        return

    # Filter files for the specific channel (e.g., C01 for channel 1)
    channel_files = [file for file in files if f'C{channel:02d}' in file]

    # print(f'# files in {s3_path}: {len(channel_files)}')

    # Download each file using threads
    with ThreadPoolExecutor() as executor:
        for remote_path in channel_files:
            file_name = os.path.basename(remote_path)
            local_path = os.path.join(download_dir, file_name)
            if not os.path.exists(local_path):
                executor.submit(download_and_crop_full_disk, fs, remote_path, local_path, spatial_resolution, variable_names)

# Function to download all files for a given day
def process_goes16_data_for_day(date, channel, download_dir, spatial_resolution, variable_names):
    # Set up S3 access
    fs = s3fs.S3FileSystem(anon=True)
    bucket = 'noaa-goes16'
    product = f'ABI-L2-CMIPF'

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
                executor.submit(process_goes16_data_for_hour, fs, s3_path, channel, download_dir, spatial_resolution, variable_names)

# Main function to process files for a range of dates
def process_goes16_data_for_period(start_date, end_date, ignored_months, channel, download_dir, crop_dir, spatial_resolution, variable_names):
    current_date = start_date
    while current_date <= end_date:
        day = current_date.strftime('%Y_%m_%d')
        if (current_date.month in ignored_months) or any(day in filename for filename in os.listdir(crop_dir)):
                print(f"Ignoring data for {day}")
                current_date += timedelta(days=1)
                continue

        print(f"Processing data for {day}")
        process_goes16_data_for_day(current_date, channel, download_dir, spatial_resolution, variable_names)

        netcdf_filename = f'{crop_dir}/C{channel:02d}_{day}.nc'
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

def crop_full_disk(full_disk_filename, spatial_resolution, variable_names, extent):
    # Explicitly choose to use exceptions
    # gdal.UseExceptions()

    cropped_content_dict = dict()

    for var in variable_names:
        # Open the file
        fd_dataset = gdal.Open(f'NETCDF:{full_disk_filename}:' + var)
        if fd_dataset is None:
            print("Unable to open the input file.")
            exit(1)

        # Read the header metadata
        metadata = fd_dataset.GetMetadata()
        scale = float(metadata.get(var + '#scale_factor'))
        offset = float(metadata.get(var + '#add_offset'))
        undef = float(metadata.get(var + '#_FillValue'))

        dtime = metadata.get('NC_GLOBAL#time_coverage_start')
        dtime = datetime.strptime(dtime, '%Y-%m-%dT%H:%M:%S.%fZ')
        yyyymmddhhmn = dtime.strftime('%Y_%m_%d_%H_%M')

        # Read the full disk data into a NumPy array
        ds = fd_dataset.ReadAsArray(0, 0, fd_dataset.RasterXSize, fd_dataset.RasterYSize).astype(float)

        # Apply the scale and offset
        ds = (ds * scale + offset)
        # print(f'offset={offset}, scale={scale}')

        # Read the original file projection and configure the output projection
        source_prj = osr.SpatialReference()
        source_prj.ImportFromProj4(fd_dataset.GetProjectionRef())

        target_prj = osr.SpatialReference()
        target_prj.ImportFromProj4("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs")

        # Reproject the data
        GeoT = fd_dataset.GetGeoTransform()
        mem_driver = gdal.GetDriverByName('MEM')
        raw = mem_driver.Create('raw', ds.shape[0], ds.shape[1], 1, gdal.GDT_Float32)
        raw.SetGeoTransform(GeoT)
        raw.GetRasterBand(1).WriteArray(ds)

        # Define the parameters for the reprojection  
        warp_options = gdal.WarpOptions(
        format='MEM', 
        srcSRS=source_prj, 
        dstSRS=target_prj,
        outputBounds=(extent[0], extent[3], extent[2], extent[1]), 
        outputBoundsSRS=target_prj, 
        outputType=gdal.GDT_Float32, 
        xRes=spatial_resolution,
        yRes=spatial_resolution,
        srcNodata=undef, 
        dstNodata='nan', 
        resampleAlg=gdal.GRA_Max
        )

        # Apply the transformation and write to the virtual dataset
        mem_dataset = gdal.Warp('', raw, options=warp_options)

        # Check if the warp operation succeeded
        if mem_dataset is None:
            print("Warping failed.")
            exit(1)

        # Now access the warped data from the virtual memory
        mem_band = mem_dataset.GetRasterBand(1)
        warped_data = mem_band.ReadAsArray()

        key = f'{var}_{yyyymmddhhmn}'
        cropped_content_dict[key] = warped_data

        # Clean up
        fd_dataset = None
        mem_dataset = None

    return cropped_content_dict


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
    Example usage:
    one-day test
    python src/goes16_sync_downloader_cropper.py --start_date "2024-02-08" --end_date "2024-02-08" --channel 7 --download_dir "./downloads" --crop_dir "./cropped" --spatial_resolution 0.1 --vars "CMI"
    '''

    # Create an argument parser to accept start and end dates, and channel number from the command line
    parser = argparse.ArgumentParser(description="Download GOES-16 data for a specific date range.")
    parser.add_argument('--start_date', type=str, required=True, help="Start date (format: YYYY-MM-DD)")
    parser.add_argument('--end_date', type=str, required=True, help="End date (format: YYYY-MM-DD)")
    parser.add_argument('--channel', type=int, required=True, help="GOES-16 channel number (1-16)")
    parser.add_argument('--download_dir', type=str, default='./downloads', help="Directory to (temporarily) save downloaded FD files")
    parser.add_argument('--crop_dir', type=str, required=True, help="Directory to save cropped files")
    parser.add_argument('--spatial_resolution', type=float, required=True, help='Spatial resolution in degrees (e.g., 0.1 for 0.1 degrees of latitude/longitude)')
    parser.add_argument("--ignored_months", nargs='+', type=int, required=False, default=[6, 7, 8],
                        help="Months to ignore (e.g., --ignored_months 6 7 8)")
    parser.add_argument("--vars", nargs='+', type=str, required=True, help="At least one variable name (CMI, ...)")

    fmt = "[%(levelname)s] %(funcName)s():%(lineno)i: %(message)s"
    logging.basicConfig(level=logging.INFO, format=fmt)

    args = parser.parse_args()

    # Parse the start and end dates
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
    channel = args.channel
    download_dir = args.download_dir
    crop_dir = args.crop_dir
    spatial_resolution = args.spatial_resolution
    ignored_months = args.ignored_months
    variable_names = args.vars

    start_time = time.time()  # Record the start time
    download_dir = './downloads'
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
    process_goes16_data_for_period(start_date, 
                                   end_date, 
                                   ignored_months, 
                                   channel = channel, 
                                   download_dir = download_dir, 
                                   crop_dir = crop_dir,
                                   spatial_resolution = spatial_resolution,
                                   variable_names = variable_names)
    end_time = time.time()  # Record the end time
    duration = (end_time - start_time) / 60  # Calculate duration in minutes
    print(f"Script execution time: {duration:.2f} minutes.")