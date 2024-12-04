import netCDF4 as nc
import numpy as np
import matplotlib.pyplot as plt
import argparse
import os
from matplotlib.animation import FuncAnimation
from PIL import Image
import cartopy.crs as ccrs  
import globals
import math

def create_animation(output_directory, output_file, global_min, global_max, fps=5):
    """
    Generates an animation (MP4 file) from PNG images in the given directory,
    with a single global scale and colorbar.

    Args:
    image_directory (str): Directory containing the PNG images.
    output_file (str): Path to save the MP4 animation.
    fps (int): Frames per second for the animation.
    """
    # Collect all image files in the directory
    image_files = sorted(
        [os.path.join(output_directory, file) for file in os.listdir(output_directory) if file.endswith('.png')]
    )
    
    if not image_files:
        print("No PNG images found in the directory!")
        return
    
    # Verify the order of images
    print(f"Found {len(image_files)} images: {image_files}")

    # Load the first image to set up the figure
    first_image = np.array(Image.open(image_files[0]))
    fig, ax = plt.subplots()
    img_plot = ax.imshow(first_image, cmap='viridis', vmin=global_min, vmax=global_max)
    ax.axis('off')  # Remove axes for better visualization

    # Add a single global colorbar
    cbar = fig.colorbar(img_plot, ax=ax, label='Intensity')
    cbar.ax.tick_params(labelsize=10)  # Adjust colorbar tick size if necessary

    def update(frame):
        """Update the image data for each frame."""
        image = np.array(Image.open(image_files[frame]))
        img_plot.set_array(image)
        return [img_plot]

    # Create the animation
    anim = FuncAnimation(fig, update, frames=len(image_files), interval=1000 / fps, blit=True)

    # Save the animation as an MP4 file
    output_file = os.path.join(output_directory, output_file)
    anim.save(output_file, fps=fps, extra_args=['-vcodec', 'libx264'])
    print(f"Animation saved to {output_file}")

def create_snapshots(netcdf_file, output_directory, title_prefix = ""):
    """
    Plots all snapshots (numpy arrays) inside the provided netCDF file and saves the results as PNG images.

    Args:
    netcdf_file (str): Path to the netCDF file containing several snapshots.
    output_directory (str): Directory where the output image files will be saved.
    """
    try:
        # Open the netCDF file
        with nc.Dataset(netcdf_file, 'r') as dataset:
            # print(f"Variables in the netCDF file: {list(dataset.variables.keys())}")
            
            global_min = float('inf')
            global_max = float('-inf')

            # Iterate through all variables in the dataset
            for variable_name in dataset.variables.keys():
                # Retrieve the data array for the variable
                data = dataset.variables[variable_name][:]

                # Compute the global min and max values for scaling
                global_min = min(global_min, data.min())
                global_max = max(global_max, data.max())

                # Generate a sanitized output file name
                sanitized_name = variable_name.replace(":", "_").replace(" ", "_")
                output_image_file = os.path.join(output_directory, f"{sanitized_name}.png")
                
                # Plot the data array and save it
                create_snapshot(data, f'{title_prefix} {variable_name}', output_image_file)
                print(f"Saved snapshot for {variable_name} to {output_image_file}")
    
            print(f"Global scale: min={global_min}, max={global_max}")

            return global_min, global_max
    except Exception as e:
        print(f"Error while processing netCDF file: {e}")

def latlon2xy(lat, lon):
    # goes_imagery_projection:semi_major_axis
    req = 6378137 # meters
    #  goes_imagery_projection:inverse_flattening
    invf = 298.257222096
    # goes_imagery_projection:semi_minor_axis
    rpol = 6356752.31414 # meters
    e = 0.0818191910435
    # goes_imagery_projection:perspective_point_height + goes_imagery_projection:semi_major_axis
    H = 42164160 # meters
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

# Function to convert lat / lon extent to GOES-16 extents
def convertExtent2GOESProjection(extent):
    # GOES-16 viewing point (satellite position) height above the earth
    GOES16_HEIGHT = 35786023.0
    # GOES-16 longitude position
    GOES16_LONGITUDE = -75.0

    a, b = latlon2xy(extent[1], extent[0])
    c, d = latlon2xy(extent[3], extent[2])
    return (a * GOES16_HEIGHT, c * GOES16_HEIGHT, b * GOES16_HEIGHT, d * GOES16_HEIGHT)

def create_snapshot(data, title, output_image_file):
    """
    Plots a data array with coastlines, borders, and gridlines, and saves it as an image.

    Args:
    data_array (numpy.ndarray): The data array to plot.
    title (str): Title of the plot.
    output_image_file (str): Path to save the plot.
    """
    # Define the projection for the plot
    # projection = ccrs.PlateCarree()

    plt.figure(figsize=(10,10))

    # Use the Geostationary projection in cartopy
    # ax = plt.axes(projection=ccrs.PlateCarree())
    ax = plt.axes(projection=ccrs.Geostationary(central_longitude=-75.0, satellite_height=35786023.0))

    img_extent = convertExtent2GOESProjection(globals.extent)

    # Create the plot
    # fig, ax = plt.subplots(subplot_kw={'projection': projection}, figsize=(10, 8))
    _ = ax.imshow(
        data,
        origin='upper',
        extent = img_extent,
        cmap='viridis',
        # transform=ccrs.PlateCarree(),
    )

    # Add coastlines, borders and gridlines
    ax.coastlines(resolution='10m', color='black', linewidth=1)
    # ax.add_feature(cfeature.BORDERS, edgecolor='black')
    # gl = ax.gridlines(crs=ccrs.PlateCarree(), 
    #                   color='gray', 
    #                   alpha=1.0, 
    #                   linestyle='--',
    #                   linewidth=0.5, 
    #                   xlocs=np.arange(-180, 180, 5), 
    #                   ylocs=np.arange(-90, 90, 5), 
    #                   draw_labels=True)
    # gl.top_labels = False
    # gl.right_labels = False

    # Add title and colorbar
    plt.title(title, fontsize=14)
    # cbar = fig.colorbar(img, ax=ax, orientation='horizontal', pad=0.05)
    # cbar.set_label('Intensity')

    # Save the plot
    plt.savefig(output_image_file, bbox_inches='tight')
    plt.close()

# def create_snapshot(data_array, title, output_image_file):
#     """
#     A placeholder function to plot a data array and save it as an image.
#     Replace this with your actual plotting logic.

#     Args:
#     data_array (numpy.ndarray): The data array to plot.
#     title (str): Title of the plot.
#     output_image_file (str): Path to save the plot.
#     """
#     import matplotlib.pyplot as plt
    
#     plt.figure()
#     plt.imshow(data_array, cmap='viridis')
#     plt.title(title)
#     # plt.colorbar(label='Intensity')
#     plt.savefig(output_image_file)
#     plt.close()

########################################################################
### MAIN
########################################################################

if __name__ == "__main__":
    '''
    python src/goes16_create_animation_from_snapshots.py --netcdf_file ./data/goes16/CMI/C07/2023/C07_2023_10_31.nc --output_dir ./C07_2023_10_31 --title_prefix Channel 07
    '''
    parser = argparse.ArgumentParser(description="Plot data from a NetCDF file.")
    parser.add_argument(
        "--netcdf_file",
        type=str,
        help="Path to the NetCDF file containing the snapshots."
    )
    parser.add_argument(
        "--title_prefix",
        default="",
        type=str,
        help="Path of directory to save the resulting PNG images and animation."
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        help="Path of directory to save the resulting PNG images and animation."
    )
    parser.add_argument(
        "--keep_snapshots",
        action="store_true",
        help="Keep the individual PNG snapshots after creating the animation."
    )
    parser.add_argument('--min_max', nargs=2, metavar=('min_value','max_value'),
                   help='Minimum and maximum values to be used to control color pallete')

    args = parser.parse_args()

    output_file = "animation.mp4"    # Name of the output MP4 file
    fps = 5                                   # Frames per second

    global_min, global_max = create_snapshots(args.netcdf_file, args.output_dir, args.title_prefix)
    if args.min_max is not None:
        global_min, global_max = args.min_max[0], args.min_max[1]

    create_animation(args.output_dir, output_file, global_min, global_max, fps)

    # Delete snapshots if --keep_snapshots is not set
    if not args.keep_snapshots:
        import os
        import glob
        snapshots = glob.glob(f"{args.output_dir}/*.png")
        for snapshot in snapshots:
            os.remove(snapshot)
        print(f"Deleted {len(snapshots)} snapshots from {args.output_dir}.")