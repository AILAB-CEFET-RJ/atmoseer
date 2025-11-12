import netCDF4 as nc
import matplotlib.pyplot as plt
import numpy as np
import cartopy.crs as ccrs
import cartopy.feature as cfeature

# Function to visualize the GOES-16 full-disk data
def visualize_goes16_file(file_path, save_path=None):
    # Open the NetCDF file
    dataset = nc.Dataset(file_path)

    # Extract data: we assume the variable of interest is 'Rad' (radiance), adjust if needed
    radiance = dataset.variables['Rad'][:]
    
    # Retrieve GOES-16 projection info and coordinates
    lat_rad = dataset.variables['goes_imager_projection'].perspective_point_height
    scale_x = dataset.variables['x'].scale_factor
    offset_x = dataset.variables['x'].add_offset
    scale_y = dataset.variables['y'].scale_factor
    offset_y = dataset.variables['y'].add_offset
    
    # Create coordinate arrays
    x = dataset.variables['x'][:] * scale_x + offset_x
    y = dataset.variables['y'][:] * scale_y + offset_y
    X, Y = np.meshgrid(x, y)
    
    # GOES-16 parameters for the satellite projection
    sat_height = dataset.variables['goes_imager_projection'].perspective_point_height
    central_lon = dataset.variables['goes_imager_projection'].longitude_of_projection_origin
    central_lat = dataset.variables['goes_imager_projection'].latitude_of_projection_origin
    
    # Close the NetCDF file
    dataset.close()

    # Create a figure for plotting
    fig = plt.figure(figsize=(12, 8))
    
    # Set up Cartopy projection
    ax = plt.axes(projection=ccrs.Geostationary(central_longitude=central_lon, satellite_height=sat_height))

    # Plot the radiance data
    plt.pcolormesh(X, Y, radiance, transform=ccrs.Geostationary(), cmap='gray', vmin=np.nanmin(radiance), vmax=np.nanmax(radiance))
    
    # Add features such as coastlines and borders for reference
    ax.add_feature(cfeature.COASTLINE, linewidth=0.5)
    ax.add_feature(cfeature.BORDERS, linewidth=0.5)
    
    # Add a colorbar for the radiance data
    plt.colorbar(label='Radiance')
    
    # Set title
    plt.title('GOES-16 Full Disk Radiance Visualization')
    
    # Show the plot
    plt.show()

    # Save the figure as an image file (if a save_path is provided)
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        print(f"Image saved as {save_path}")

if __name__ == "__main__":
    # Example file path to a downloaded GOES-16 full disk file (replace with the correct file path)
    file_path = './goes16_data/OR_ABI-L1b-RadF-M6C10_G16_s20233222350206_e20233222359526_c20233222359549.nc'
    # file_path = input("Enter the GOES16 full disk filename: ")

    # Provide the save path for the image (adjust the filename and extension as needed)
    save_image_path = 'goes16_full_disk_image.png'

    # Call the visualization function
    visualize_goes16_file(file_path, save_path=save_image_path)
