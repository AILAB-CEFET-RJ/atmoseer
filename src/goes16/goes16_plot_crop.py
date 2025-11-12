# %load ../../src/goes16_plot_crop.py
from datetime import datetime
import re
from netCDF4 import Dataset                     # Read / Write NetCDF4 files
import matplotlib.pyplot as plt                 # Plotting library
from datetime import datetime                   # Basic Dates and time types
import cartopy, cartopy.crs as ccrs             # Plot maps
import os                                       # Miscellaneous operating system interfaces
from osgeo import osr                           # Python bindings for GDAL
from osgeo import gdal                          # Python bindings for GDAL
import numpy as np                              # Scientific computing with Python

def change_extension_to_jpg(filepath: str) -> str:
    # Use os.path.splitext to separate the file root and extension
    file_root, _ = os.path.splitext(filepath)
    # Append the new '.jpg' extension
    return f"{file_root}.jpg"

def extract_timestamp(file_path: str) -> str:
    # Use regular expressions to capture the date and time in the file path
    match = re.search(r'(\d{4})_(\d{2})_(\d{2})_(\d{2})_(\d{2})', file_path)
    
    if match:
        # Extract the date and time components
        year, month, day, hour, minute = match.groups()
        
        # Create a datetime object
        dt = datetime(year=int(year), month=int(month), day=int(day), hour=int(hour), minute=int(minute))
        
        # Format the datetime object into the desired output format
        return dt.strftime("%d/%m/%Y %H:%M")
    else:
        raise ValueError("No valid timestamp found in the file path.")

def extract_band_number(file_path):
    # Use regular expression to find the 'band' followed by digits
    match = re.search(r'band(\d+)', file_path)
    
    if match:
        # Return the band number as an integer
        return int(match.group(1))
    else:
        # Return None if no match is found
        return None

def convert_timestamp(timestamp: str) -> str:
    # Convert the timestamp to a datetime object
    dt = datetime.strptime(timestamp, '%Y%m%d%H%M')
    # Return the date and time in the format YYYY-MM-DD HH:MM:SS
    return dt.strftime('%Y-%m-%d %H:%M:%S')

def plot_crop(filename: str, save_fig: bool = False):
 
    # Open the reprojected GOES-R image
    file = Dataset(filename)

    lat_max, lon_max = (
            -21.699774257353113,
            -42.35676996062447,
        )  # canto superior direito
    lat_min, lon_min = (
            -23.801876626302175,
            -45.05290312102409,
        )  # canto inferior esquerdo

    extent = [lon_min, lat_min, lon_max, lat_max]

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
    colormap = "gray_r" # White to black for IR channels
        
    # Plot the image
    img = ax.imshow(data, origin='upper', extent=img_extent, cmap=colormap)

    # Add coastlines, borders and gridlines
    ax.coastlines(resolution='10m', color='white', linewidth=0.8)
    ax.add_feature(cartopy.feature.BORDERS, edgecolor='white', linewidth=0.5)
    gl = ax.gridlines(crs=ccrs.PlateCarree(), color='gray', alpha=1.0, linestyle='--', linewidth=0.25, xlocs=np.arange(-180, 180, 5), ylocs=np.arange(-90, 90, 5), draw_labels=True)
    gl.top_labels = False
    gl.right_labels = False

    # Add a colorbar
    plt.colorbar(img, label='Brightness Temperatures (°C)', extend='both', orientation='horizontal', pad=0.05, fraction=0.05)

    # Extract date
    dtime = extract_timestamp(filename)

    band_number = extract_band_number(filename)

    # Add a title
    plt.title('GOES-16 Band ' + str(band_number) + ' at ' + dtime + ' UTC\n', fontweight='bold', fontsize=10, loc='left')
    plt.title('Reg.: ' + str(extent) , fontsize=10, loc='right')

    if save_fig:
        #-----------------------------------------------------------------------------------------------------------
        # Save the image
        fig_filename = change_extension_to_jpg(filename)
        plt.savefig(fig_filename, bbox_inches='tight', pad_inches=0, dpi=300)

    # Show the image
    plt.show()