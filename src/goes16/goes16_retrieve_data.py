'''
Retrieve GOES16 Data Script
 Command line inputs:
    -p: Product Name
    -di: Initial date
    -df: Final date
    -l: Extent (Min lon, Min lat, Max lon, Max lat)

  Example:
    python src/retrieve_goes16_data.py -p ABI-L2-DSIF -di 20200201 -df 20200201 -l -74.0 34.1 -34.8 5.5
'''

# Required modules

from datetime import timedelta, datetime  # Basic Dates and time types
import os
import sys
import argparse
import boto3  # type: ignore
from botocore import UNSIGNED  # type: ignore # boto3 config
from botocore.config import Config  # type: ignore
from osgeo import gdal                          # Python bindings for GDAL
# from goes16_utils import reproject
from netCDF4 import Dataset  # type: ignore
import math
import pickle

gdal.PushErrorHandler('CPLQuietErrorHandler')   # Ignore GDAL warnings


def get_sub_folder_list(prefix):

    s3_result = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter="/")
    list_of_subfolders = []
    for result in s3_result['CommonPrefixes']:
        list_of_subfolders.append(result['Prefix'].replace(prefix, '').replace('/', ''))

    return list_of_subfolders


def download_full_disk(product_name, year, day_of_year, hour, minute, path_dest):

    prefix = f'{product_name}/{year}/{day_of_year}/{hour}/OR_{product_name}-M6_G16_s{year}{day_of_year}{hour}{minute}'

    s3_result = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter="/")

    # Check if there are files available
    if 'Contents' not in s3_result:
        # There are no files
        print(f'No files found for the date. Product: {product_name}')
        return None
    else:
        # There are files
        for obj in s3_result['Contents']:
            key = obj['Key']
            # Print the file name
            file_name = key.split('/')[-1].split('.')[0]

            # Download the file
            if os.path.exists(f'{path_dest}/{file_name}.nc'):
                print(f'File {path_dest}/{file_name}.nc exists')
                return None
            else:
                print(f'Downloading file {path_dest}/{file_name}.nc')
                s3_client.download_file(bucket_name, key, f'{path_dest}/{file_name}.nc')
    return f'{file_name}'


def evaluate_year_limits(list_to_filter, initial_date, final_date):

    if initial_date is not None:
        year_initial_date = datetime.strptime(initial_date, '%Y%m%d').strftime('%Y')
        list_to_filter = list(filter(lambda x: x >= year_initial_date, list_to_filter))

    if final_date is not None:
        year_final_date = datetime.strptime(final_date, '%Y%m%d').strftime('%Y')
        list_to_filter = list(filter(lambda x: x <= year_final_date, list_to_filter))
    return list_to_filter


def evaluate_day_of_year_limits(year, list_to_filter, initial_date, final_date):

    if initial_date is not None:
        day_of_initial_date = datetime.strptime(initial_date, '%Y%m%d').strftime('%j')
        year_initial_date = datetime.strptime(initial_date, '%Y%m%d').strftime('%Y')
        if year == year_initial_date:
            list_to_filter = list(filter(lambda x: x >= day_of_initial_date, list_to_filter))

    if final_date is not None:
        day_of_final_date = datetime.strptime(final_date, '%Y%m%d').strftime('%j')
        year_final_date = datetime.strptime(final_date, '%Y%m%d').strftime('%Y')
        if year == year_final_date:
            list_to_filter = list(filter(lambda x: x <= day_of_final_date, list_to_filter))
    return list_to_filter


def geo2grid(lat, lon, nc):

    # Apply scale and offset
    xscale, xoffset = nc.variables['x'].scale_factor, nc.variables['x'].add_offset
    yscale, yoffset = nc.variables['y'].scale_factor, nc.variables['y'].add_offset
    x, y = latlon2xy(lat, lon)
    col = (x - xoffset)/xscale
    lin = (y - yoffset)/yscale
    return int(lin), int(col)


def latlon2xy(lat, lon):
    # goes_imagery_projection:semi_major_axis
    req = 6378137  # meters
    #  goes_imagery_projection:inverse_flattening
    # invf = 298.257222096
    # goes_imagery_projection:semi_minor_axis
    rpol = 6356752.31414  # meters
    e = 0.0818191910435
    # goes_imagery_projection:perspective_point_height + goes_imagery_projection:semi_major_axis
    H = 42164160  # meters
    # goes_imagery_projection: longitude_of_projection_origin
    lambda0 = -1.308996939

    # Convert to radians
    latRad = lat * (math.pi/180)
    lonRad = lon * (math.pi/180)

    # (1) geocentric latitude
    Phi_c = math.atan(((rpol * rpol)/(req * req)) * math.tan(latRad))
    # (2) geocentric distance to the point on the ellipsoid
    rc = rpol/(math.sqrt(1 - ((e * e) * (math.cos(Phi_c) * math.cos(Phi_c)))))
    # (3) sx
    sx = H - (rc * math.cos(Phi_c) * math.cos(lonRad - lambda0))
    # (4) sy
    sy = -rc * math.cos(Phi_c) * math.sin(lonRad - lambda0)
    # (5)
    sz = rc * math.sin(Phi_c)

    # x,y
    x = math.asin((-sy)/math.sqrt((sx*sx) + (sy*sy) + (sz*sz)))
    y = math.atan(sz/sx)

    return x, y


def obtain_index_values(path, file_name, date, extent):

    file = Dataset(path + file_name)

    print(file)

    # Convert lat/lon to grid-coordinates
    lly, llx = geo2grid(extent[1], extent[0], file)
    ury, urx = geo2grid(extent[3], extent[2], file)

    print(lly, llx, ury, urx)
    dict_indices = {}
    for instability_index_name in ['CAPE', 'LI', 'TT', 'SI', 'KI']:
        # Get the pixel values
        data = file.variables[instability_index_name][ury:lly, llx:urx]
        dict_indices[instability_index_name] = data

    data = file.variables['DQF_Overall'][ury:lly, llx:urx]
    dict_indices['DQF_Overall'] = data

    # open a file, where you want to store the data
    # temp = os.path.splitext(file_name)  # given '/home/user/somefile.nc',  returns ('/home/user/somefile', '.nc')
    pkl_file = open(f'{path + date}.pkl', 'wb')

    # dump information to that file
    pickle.dump(dict_indices, pkl_file)

    # close the file
    pkl_file.close()

def main(argv):
    # ---------------------------------------------------------------------------------------------------------------
    # Command line argument section
    parser = argparse.ArgumentParser(prog=argv[0],
                                     description="""This script provides a simple interface for retrieve the partial
                                     or complete temporal series of a GOES16 Product.""")
    parser.add_argument("-p", "--product_name", required=True, help="product name", metavar='')
    parser.add_argument('-di', "--initial_date", help='Initial date to retrieve data', required=False)
    parser.add_argument('-df', "--final_date", help='Final date to retrieve data', required=False)
    parser.add_argument('-l', "--latlonlist", nargs='+', help='<Required> Set min and max latitude and' +
                        'longitude values', required=True)
    args = parser.parse_args(argv[1:])

    product_name = args.product_name
    initial_date = args.initial_date
    final_date = args.final_date
    extent = [float(x) for x in args.latlonlist]

    minutes = [0, 10, 20, 30, 40, 50]
    years = get_sub_folder_list(product_name + "/")
    years = evaluate_year_limits(years, initial_date, final_date)
    for year in years:
        days = get_sub_folder_list(product_name + "/" + year + "/")
        days = evaluate_day_of_year_limits(year, days, initial_date, final_date)

        for day_of_year in days:
            hours = get_sub_folder_list(product_name + "/" + year + "/" + day_of_year + "/")

            for hour in hours:
                for minute in minutes:
                    yyyy_mm_dd = datetime(int(year), 1, 1, int(hour), minute) + timedelta(int(day_of_year) - 1)
                    current_date_string = datetime.strptime(str(yyyy_mm_dd), '%Y-%m-%d %H:%M:%S').strftime('%Y%m%d%H%M')

                    if not os.path.exists(data_dir + current_date_string + '.pkl'):
                        full_disk_downloaded = download_full_disk(product_name, year, day_of_year,
                                                                  hour, minute, data_dir)

                        if full_disk_downloaded is not None:
                            print("Retrieving data for " + str(yyyy_mm_dd))
                            current_date_string = datetime.strptime(str(yyyy_mm_dd), '%Y-%m-%d %H:%M:%S').strftime('%Y%m%d%H%M')

                            obtain_index_values(data_dir, full_disk_downloaded + '.nc', current_date_string, extent)
                            os.remove(data_dir + "/" + full_disk_downloaded + '.nc')


if __name__ == "__main__":
    data_dir = "./data/goes16/dsif/"
    bucket_name = 'noaa-goes16'
    s3_client = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    main(sys.argv)
