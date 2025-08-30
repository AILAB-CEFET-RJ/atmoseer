import matplotlib.pyplot as plt
import numpy as np
import matplotlib.animation as animation
import pickle
from datetime import datetime, timedelta
import cartopy, cartopy.crs as ccrs  
import math

def min_max_normalize_masked_array(masked_array: np.ma.MaskedArray):
    # Ensure the input is a MaskedArray
    if not isinstance(masked_array, np.ma.MaskedArray):
        raise ValueError("Input must be a numpy.ma.MaskedArray")

    # Calculate the min and max values ignoring the masked elements
    min_val = masked_array.min()
    max_val = masked_array.max()

    # Avoid division by zero in case max_val equals min_val
    if max_val == min_val:
        return np.ma.masked_array(np.zeros_like(masked_array), mask=masked_array.mask)

    # Apply the min-max normalization
    normalized_array = (masked_array - min_val) / (max_val - min_val)

    return normalized_array

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

def generate_timestamps(initial_timestamp, final_timestamp, interval_in_minutes = 10):
    # Parse the input timestamps to datetime objects
    initial_dt = datetime.strptime(initial_timestamp, '%Y%m%d%H%M')
    final_dt = datetime.strptime(final_timestamp, '%Y%m%d%H%M')

    # Generate timestamps in 10-minute intervals
    timestamps = []
    current_dt = initial_dt

    while current_dt <= final_dt:
        timestamps.append(current_dt.strftime('%Y%m%d%H%M'))
        current_dt += timedelta(minutes=interval_in_minutes)

    return timestamps

def get_frame(data, extent):
    #-----------------------------------------------------------------------------------------------------------
    # Compute data-extent in GOES projection-coordinates
    img_extent = convertExtent2GOESProjection(extent)
    #-----------------------------------------------------------------------------------------------------------
    # Choose the plot size (width x height, in inches)
    # plt.figure(figsize=(10,10))

    # Use the Geostationary projection in cartopy
    ax = plt.axes(projection=ccrs.Geostationary(central_longitude=-75.0, satellite_height=35786023.0))

    # Define the color scale based on the channel
    colormap = "jet" # White to black for IR channels

    # Plot the image
    img = ax.imshow(data, origin='upper', extent=img_extent, cmap=colormap, animated = True)

    # Add coastlines, borders and gridlines
    ax.coastlines(resolution='10m', color='black', linewidth=0.8)
    ax.add_feature(cartopy.feature.BORDERS, edgecolor='black', linewidth=0.5)
    ax.gridlines(color='black', alpha=0.5, linestyle='--', linewidth=0.5)

    # Add a colorbar
    # plt.colorbar(img, label='CAPE', extend='both', orientation='horizontal', pad=0.05, fraction=0.05)

    # plt.close()
    
    return img
    return plt.imgshow()

def gen_animation(initial_timestamp: str, final_timestamp: str, input_folder: str, product_name: str, extent: list, out_file_name: str):
    def update_frame(frame_number, img, images):
        """Update the image for each frame of the animation."""
        img.set_array(images[frame_number])
        return img,
    
    # fig = plt.figure()

    i_timestamp = int(initial_timestamp)
    f_timestamp = int(final_timestamp)

    assert i_timestamp <= f_timestamp
    
    timestamps = generate_timestamps(initial_timestamp, final_timestamp, interval_in_minutes = 10)

    # images is a list of artists to draw at each frame
    images = []
    for timestamp in timestamps:
        # print(f'Current timestamp: {timestamp}')

        filename = f'{input_folder}/diff_{timestamp}.pkl'

        # open a file containing the pickled data
        file = open(filename, 'rb')

        pickled_data = pickle.load(file)

        # close the file
        file.close()

        data = pickled_data[product_name]

        data = min_max_normalize_masked_array(data)


        images += [data]

    num_images = len(images)
    print(num_images)

    # Create a figure and axis
    fig, ax = plt.subplots()

    # Use the Geostationary projection in cartopy
    ax = plt.axes(projection=ccrs.Geostationary(central_longitude=-75.0, satellite_height=35786023.0))
    ax.set_axis_off()

    # See https://stackoverflow.com/questions/67855367/remove-axis-from-animation-artistanimation-python
    # ax = fig.add_subplot(111)
    ax.get_xaxis().set_visible(False)
    ax.get_yaxis().set_visible(False)
    # ax.axis('off')

    # Add coastlines, borders and gridlines
    ax.coastlines(resolution='10m', color='black', linewidth=0.8)
    ax.add_feature(cartopy.feature.BORDERS, edgecolor='black', linewidth=0.5)
    #ax.gridlines(color='black', alpha=0.5, linestyle='--', linewidth=0.5)


    img_extent = convertExtent2GOESProjection(extent)


    img = ax.imshow(images[0], interpolation='none', extent=img_extent, cmap="gray",origin='upper')
    ax.coastlines(resolution='10m', color='yellow', linewidth=0.8)
    plt.axis('off')

    # Configurar o writer
    Writer = animation.writers['ffmpeg']
    writer = Writer(fps=15, metadata=dict(artist='Me'), bitrate=1800)
    plt.colorbar(img, label=f'Profundidade das nuvens - {initial_timestamp[:4]}-{initial_timestamp[4:6]}-{initial_timestamp[6:8]}', extend='both', orientation='vertical', pad=0.05, fraction=0.05)

    # Create the animation
    ani = animation.FuncAnimation(fig, update_frame, frames=num_images, fargs=(img, images), blit=True)

    # Save the animation
    ani.save(out_file_name, writer=writer)
    print(f'Created MP4 file: {out_file_name}')

extent = [-43.890602827150, -23.1339033365138, -43.0483514573222, -22.64972474827293]

gen_animation(initial_timestamp = '202310310000', 
                final_timestamp = '202310312300', 
                input_folder = 'animation_input/20231031', 
                product_name = 'Band1', 
                extent = extent, 
                out_file_name = 'profundidade_nuvens_2023_10_31.mp4')