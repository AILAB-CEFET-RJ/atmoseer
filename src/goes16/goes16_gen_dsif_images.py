import os
from netCDF4 import Dataset                     # Read / Write NetCDF4 files
import matplotlib.pyplot as plt                 # Plotting library
from datetime import datetime                   # Basic Dates and time types
import cartopy, cartopy.crs as ccrs             # Plot maps
import os                                       # Miscellaneous operating system interfaces
from osgeo import osr                           # Python bindings for GDAL
from osgeo import gdal                          # Python bindings for GDAL
import numpy as np                              # Scientific computing with Python

"""Builds a reprojected netCDF file from a full disk netCDF file.

Args:
in_filename (str): Path to the full disk netCDF file.
out_filename (str): Path to write the reprojected netCDF file.
var (str): Name of the variable to reproject within the input file.
extent (tuple): A tuple of four floats defining the output bounding box in
geographic coordinates (lon_min, lat_max, lon_max, lat_min).

Returns:
str: The starting time of the data coverage extracted from the metadata
of the input file.

Raises:
RuntimeError: If there is an error opening the input file or writing
the reprojected file.
"""
def build_projection_from_full_disk(in_filename, out_filename, var, extent):

    # Open the file
    img = gdal.Open(f'NETCDF:{in_filename}:' + var)

    # Read the header metadata
    metadata = img.GetMetadata()
    scale = float(metadata.get(var + '#scale_factor'))
    offset = float(metadata.get(var + '#add_offset'))
    undef = float(metadata.get(var + '#_FillValue'))
    dtime = metadata.get('NC_GLOBAL#time_coverage_start')

    # print(f'scale/offset/undef/dtime: {scale}/{offset}/{undef}/{dtime}')

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
            xRes = 0.02,
            yRes = 0.02,
            resampleAlg = gdal.GRA_NearestNeighbour)

    # print(options)

    # Write the reprojected file on disk
    gdal.Warp(f'{out_filename}', raw, options=options)
    print(f'Projection saved to file {out_filename}.')

    return dtime

def build_image_from_projection(projection_filename, var, dtime):
    #-----------------------------------------------------------------------------------------------------------
    # Open the reprojected GOES-R image
    file = Dataset(f'{projection_filename}')

    # print(f'file.variables = {file.variables}')

    # Get the pixel values
    data = file.variables['Band1'][:]

    #-----------------------------------------------------------------------------------------------------------
    # Choose the plot size (width x height, in inches)
    plt.figure(figsize=(10,10))

    # Use the Geostationary projection in cartopy
    ax = plt.axes(projection=ccrs.PlateCarree())

    # Define the image extent
    img_extent = [extent[0], extent[2], extent[1], extent[3]]

    # Define the color scale based on the channel
    colormap = "jet" # White to black for IR channels
    # colormap = "gray_r" # White to black for IR channels

    # Plot the image
    img = ax.imshow(data, origin='upper', extent=img_extent, cmap=colormap)

    # Add coastlines, borders and gridlines
    ax.coastlines(resolution='10m', color='black', linewidth=0.8)
    ax.add_feature(cartopy.feature.BORDERS, edgecolor='white', linewidth=0.5)
    gl = ax.gridlines(crs=ccrs.PlateCarree(), 
                        color='gray', 
                        alpha=1.0, 
                        linestyle='--', 
                        linewidth=0.25, 
                        xlocs=np.arange(-180, 180, 5), 
                        ylocs=np.arange(-90, 90, 5), 
                        draw_labels=True)
    gl.top_labels = False
    gl.right_labels = False

    # Add a colorbar
    plt.colorbar(img, label=var, extend='both', orientation='horizontal', pad=0.05, fraction=0.05)

    # Extract date
    date = (datetime.strptime(dtime, '%Y-%m-%dT%H:%M:%S.%fZ'))

    # Add a title
    plt.title('GOES-16 ' + var + ' ' + date.strftime('%Y-%m-%d %H:%M') + ' UTC', fontweight='bold', fontsize=10, loc='left')
    plt.title('Reg.: ' + str(extent) , fontsize=10, loc='right')


    #-----------------------------------------------------------------------------------------------------------
    # Save the image
    temp = file_name[0:-3]
    plt.savefig(f'{output_folder}{temp}_{var}.png', bbox_inches='tight', pad_inches=0, dpi=300)


if __name__ == "__main__":
    input_folder    = './data/goes16/goes16_dsif/'
    output_folder   = './data/goes16/goes16_dsif_imgs/'

    # Get the list of file names in the folder
    file_names = [f for f in os.listdir(input_folder) if os.path.isfile(os.path.join(input_folder, f))]

    # Coordinates of rectangle for region of interest (Rio de Janeiro municipality)
    extent = [-64.0, -35.0, -35.0, -15.0] # Min lon, Min lat, Max lon, Max lat

    # Options:
    # - Convective Available Potential Energy (CAPE)
    # - Lifted Index (LI)
    # - Total Totals (TT)
    # - Showalter Index (SI)
    # - K-index (KI)

    vars = ['CAPE', 'LI', 'TT', 'SI', 'KI']

    file_names.sort()

    for file_name in file_names:
        print(f'Generating images for {file_name}')
        for var in vars:
            in_filename = f'{input_folder}{file_name}'
            out_filename = file_name[0:-3]
            out_filename = f'{output_folder}{out_filename}_reprojected.nc'
            print(out_filename)
            dtime = build_projection_from_full_disk(in_filename, out_filename, var, extent)
            build_image_from_projection(out_filename, var, dtime)