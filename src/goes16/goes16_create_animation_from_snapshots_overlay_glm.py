import netCDF4 as nc
import numpy as np
import matplotlib.pyplot as plt
import argparse
import os
from matplotlib.animation import FuncAnimation
from PIL import Image
import cartopy.crs as ccrs  
from config import globals
import math
from matplotlib.patches import Circle

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

def create_snapshots(netcdf_file, glm_netcdf_file, output_directory, title_prefix = ""):
    """
    Plots all snapshots (numpy arrays) inside the provided netCDF file and saves the results as PNG images.

    Args:
    netcdf_file (str): Path to the netCDF file containing several snapshots.
    output_directory (str): Directory where the output image files will be saved.
    """
    try:
        # Open the netCDF file
        print(f'Opening file {netcdf_file}')
        with nc.Dataset(netcdf_file, 'r') as dataset, nc.Dataset(glm_netcdf_file, 'r') as glm_dataset:
            # print(f"Variables in the netCDF file: {list(dataset.variables.keys())}")
            
            global_min = float('inf')
            global_max = float('-inf')

            # Iterate through all variables in the dataset
            for variable_name in dataset.variables.keys():
                # Retrieve the data array for the variable
                data = dataset.variables[variable_name][:]
                if variable_name[4:] in glm_dataset.variables:
                    overlay_data = glm_dataset.variables[variable_name[4:]][:]
                else:
                    print(f"Variable {variable_name} not found in GLM dataset, skipping.")
                    continue

                # Compute the global min and max values for scaling
                global_min = min(global_min, data.min())
                global_max = max(global_max, data.max())

                # Generate a sanitized output file name
                sanitized_name = variable_name.replace(":", "_").replace(" ", "_")
                output_image_file = os.path.join(output_directory, f"{sanitized_name}.png")
                
                # Plot the data array and save it
                print(f'data.shape for variable {variable_name}: {data.shape}')
                create_snapshot_with_overlay(data, overlay_data, f'{title_prefix} {variable_name}', output_image_file)
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

def create_snapshot_with_overlay(data, overlay_data, title, output_image_file):
    """
    Plots a data array with coastlines, borders, gridlines, and overlay points, and saves it as an image.

    Args:
    data (numpy.ndarray): The main data array to plot.
    overlay_data (numpy.ndarray): The overlay data array for plotting red circles.
    title (str): Title of the plot.
    output_image_file (str): Path to save the plot.
    """

    if data.shape != overlay_data.shape:
        raise ValueError(f"The overlay shape {overlay_data.shape} must be the same as the GLM array shape {data.shape}.")
    
    # Define the projection for the plot
    fig, ax = plt.subplots(subplot_kw={'projection': ccrs.Geostationary(central_longitude=-75.0, satellite_height=35786023.0)})
    
    # Define the extent of the plot (use actual geographical extents for your data)
    img_extent = convertExtent2GOESProjection(globals.extent)

    # Plot the main data array
    img = ax.imshow(data, origin='upper', extent=img_extent, cmap='viridis')

    # Add coastlines
    ax.coastlines(resolution='10m', color='black', linewidth=1)

    # Overlay points (draw circles for non-zero values)
    rows, cols = overlay_data.shape
    for row in range(rows):
        for col in range(cols):
            value = overlay_data[row, col]
            if value > 0:  # Only draw circles for non-zero values
                # Convert array indices to geographic coordinates (adjust to your projection)
                x = img_extent[0] + (img_extent[1] - img_extent[0]) * col / cols
                y = img_extent[2] + (img_extent[3] - img_extent[2]) * row / rows
                # Scale circle size by value (adjust multiplier as needed)
                circle_size = value * 250
                circle = Circle((x, y), radius=circle_size, color='red', transform=ax.transData)
                ax.add_patch(circle)

    # Add title and save the plot
    plt.title(title, fontsize=14)
    plt.savefig(output_image_file, bbox_inches='tight')
    plt.close()

########################################################################
### MAIN
########################################################################

if __name__ == "__main__":
    '''
    python src/goes16_create_animation_from_snapshots_overlay_glm.py --netcdf_file ./features/CMI/profundidade_nuvens/2020/PN_2020_03_02.nc 
                                                                    --glm_netcdf_file ./data/goes16/GLM/aggregated_data/2020/03/2020-03-02.nc  
                                                                    --output_dir ./anim/PN_GLM --title_prefix PN_GLM
    '''
    parser = argparse.ArgumentParser(description="Plot data from a NetCDF file.")
    parser.add_argument(
        "--netcdf_file",
        type=str,
        help="Path to the NetCDF file containing the snapshots."
    )
    parser.add_argument(
        "--glm_netcdf_file",
        type=str,
        help="Path to the GLM NetCDF file containing the snapshots."
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

    global_min, global_max = create_snapshots(args.netcdf_file, args.glm_netcdf_file, args.output_dir, args.title_prefix)
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