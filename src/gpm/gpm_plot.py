import os
import numpy as np
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import xarray as xr
import imageio

def create_imerg_animation(input_dir, output_file):
    # Ensure the output directory exists
    print(f"Output file: {output_file}")
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # Get all NetCDF files in the input directory
    files = sorted([os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith('.nc')])
    if not files:
        print("No IMERG NetCDF files found in the input directory.")
        return

    # List to store temporary frame files
    frames = []

    # Create a figure for visualization
    fig, ax = plt.subplots(subplot_kw={'projection': ccrs.PlateCarree()}, figsize=(10, 6))

    for file in files:
        # Load data from NetCDF
        print(f"Opening file {file}")
        data = xr.open_dataset(file)
        lats = data['lat'].values
        lons = data['lon'].values
        precipitation = data['precipitationCal'].values[0]  # Assuming time is the first dimension

        # Plot data
        ax.clear()
        ax.set_extent([lons.min(), lons.max(), lats.min(), lats.max()])
        ax.add_feature(cfeature.COASTLINE)
        ax.add_feature(cfeature.BORDERS, linestyle=':')
        ax.add_feature(cfeature.LAND, edgecolor='black')
        im = ax.pcolormesh(lons, lats, precipitation, transform=ccrs.PlateCarree(), cmap='coolwarm')
        plt.colorbar(im, ax=ax, orientation='horizontal', pad=0.05, label="Precipitation (mm/h)")
        plt.title(f"GPM IMERG: {data.time.values[0]}")  # Adjust as per dataset time format

        # Save the frame
        frame_file = f"frame_{len(frames):04d}.png"
        plt.savefig(frame_file)
        frames.append(frame_file)

        # Close the dataset to save memory
        data.close()

    # Create MP4 animation using imageio
    with imageio.get_writer(output_file, fps=2) as writer:
        for frame in frames:
            writer.append_data(imageio.imread(frame))

    # Clean up temporary frames
    for frame in frames:
        os.remove(frame)

    print(f"Animation saved as {output_file}")


# Example usage
input_directory = "./data/GPM/2024"
output_animation = "./imerg_animation.mp4"
create_imerg_animation(input_directory, output_animation)
