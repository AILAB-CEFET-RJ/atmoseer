import argparse
from glob import glob
import os
import re
import numpy as np
import pandas as pd
import xarray as xr


def process_feature(feature_name, file_pattern):
    global combined_ds
    timestamps = []
    data_list = []

    # Find all files for the current feature
    files = sorted(glob(file_pattern))
    for file_path in files:
        ds = xr.open_dataset(file_path)

        for var_name in ds.variables:
            # Match the timestamp pattern
            match = re.search(r'(?:CMI_)?(\d{4}_\d{2}_\d{2}_\d{2}_\d{2})', var_name)

            if match:
                # Parse timestamp
                timestamp_str = match.group(1)
                timestamp = np.datetime64(
                    f"{timestamp_str[:4]}-{timestamp_str[5:7]}-{timestamp_str[8:10]}T{timestamp_str[11:13]}:{timestamp_str[14:]}"
                )

                minutes = int(timestamp_str[14:])
                if minutes not in [0, 30]:
                    continue  # Skip timestamps not on :00 or :30

                timestamps.append(timestamp)

                # Extract data and ensure it matches the required shape
                data = ds[var_name].values  # Shape: (lat, lon)
                if data.shape != (lat_dim, lon_dim):
                    raise ValueError(f"Unexpected data shape: {data.shape}")

                # Add data to the list
                data_list.append(data)

    if data_list:
        # Stack data along the time dimension
        feature_data = np.stack(data_list, axis=0)  # Shape: (time, lat, lon)

        # Expand to include the channel dimension
        feature_data = np.expand_dims(feature_data, axis=-1)  # Shape: (time, lat, lon, 1)

        # Create a dataset for this feature
        feature_ds = xr.Dataset(
            {
                "data": (("time", "lat", "lon", "channel"), feature_data),
            },
            coords={
                "time": timestamps,
                "lat": np.arange(lat_dim),
                "lon": np.arange(lon_dim),
                "channel": [feature_name],
            },
        )

        # Merge this feature's dataset with the combined dataset
        if combined_ds.dims["time"] == 0:  # If combined_ds is still empty
            combined_ds = feature_ds
        else:
            # Align time and concatenate along the channel dimension
            combined_ds = xr.concat([combined_ds, feature_ds], dim="channel")


def process_target(target_name, file_pattern):
    global target_ds
    timestamps = []
    data_list = []

    files = sorted(glob(file_pattern))
    for file_path in files:
        ds = xr.open_dataset(file_path)

        for var_name in ds.variables:
            # Match the timestamp pattern
            match = re.search(r'(?:IMERG_)?(\d{4}_\d{2}_\d{2}_\d{2}_\d{2})', var_name)

            if match:
                # Parse timestamp
                timestamp_str = match.group(1)
                timestamp = np.datetime64(
                    f"{timestamp_str[:4]}-{timestamp_str[5:7]}-{timestamp_str[8:10]}T{timestamp_str[11:13]}:{timestamp_str[14:]}"
                )

                minutes = int(timestamp_str[14:])
                if minutes not in [0, 30]:
                    continue  # Skip timestamps not on :00 or :30

                timestamps.append(timestamp)

                # Extract data and ensure it matches the required shape
                data = ds[var_name].values  # Shape: (lat, lon)
                if data.shape != (lat_dim, lon_dim):
                    raise ValueError(f"Unexpected data shape: {data.shape}")

                # Add data to the list
                data_list.append(data)

    if data_list:
        # Stack data along the time dimension
        feature_data = np.stack(data_list, axis=0)  # Shape: (time, lat, lon)

        # Expand to include the channel dimension
        feature_data = np.expand_dims(feature_data, axis=-1)  # Shape: (time, lat, lon, 1)

        # Create a dataset for this feature
        feature_ds = xr.Dataset(
            {
                "data": (("time", "lat", "lon", "channel"), feature_data),
            },
            coords={
                "time": timestamps,
                "lat": np.arange(lat_dim),
                "lon": np.arange(lon_dim),
                "channel": [target_name],
            },
        )

        # Merge this feature's dataset with the combined dataset
        if target_ds.dims["time"] == 0:  # If target_ds is still empty
            target_ds = feature_ds
        else:
            # Align time and concatenate along the channel dimension
            target_ds = xr.concat([target_ds, feature_ds], dim="channel")


# Function to check for maximum gap within a sample
def check_max_gap(timestamps):
    """Check the maximum time difference (gap) in minutes within a sample."""
    time_deltas = timestamps.diff(dim="time") / np.timedelta64(1, "m")  # Convert to minutes
    max_gap = time_deltas.max().item() if len(time_deltas) > 0 else 0
    return max_gap


# Function to collect samples
def collect_samples(features_ds, target_ds, timestep, max_gap):
    """Collect valid X_samples and Y_samples from features_ds and target_ds."""
    X_samples, Y_samples = [], []
    times = features_ds.time  # Keep as xarray.DataArray to use diff()
    
    # Iterate through the features_ds in sliding windows
    for i in range(len(times) - timestep):
        
        time_window_x = times.isel(time=slice(i, i + timestep))
        time_window_y = times.isel(time=slice(i + 1, i + timestep + 1))

        # Check for gaps in X_sample's time window
        if check_max_gap(time_window_x) > max_gap:
            continue  # Skip samples with large gaps
        if check_max_gap(time_window_y) > max_gap:
            continue  # Skip samples with large gaps
        
        # Extract X and Y samples
        X_sample = features_ds.isel(time=slice(i, i + timestep)).data
        Y_sample = target_ds.isel(time=slice(i + 1, i + timestep + 1)).data

        X_samples.append(X_sample)
        Y_samples.append(Y_sample)
    
    return X_samples, Y_samples


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Consolidate multiple feature files into a single dataset and then collect samples for STConvS2S")
    parser.add_argument(
        "--features-path", type=str, 
        default="./src/features",
        help="Path to the directory containing the feature files"
    )

    parser.add_argument(
        "--target-path", type=str, 
        default="./target",
        help="Path to the directory containing the target files"
    )

    parser.add_argument(
        "--output-path", type=str, default="./src/output_dataset.nc",
        help="Path to save the consolidated dataset with samples"
    )

    parser.add_argument(
        "--lat-dim", type=int, required=True,
        help="Number of latitude points in the feature files"
    )

    parser.add_argument(
        "--lon-dim", type=int, required=True,
        help="Number of longitude points in the feature files"
    )

    parser.add_argument(
        "--max-gap", type=int, default=30,
        help="Maximum allowed gap (in minutes) between timestamps in a sample"
    )

    parser.add_argument(
        "--timestep", type=int, default=5,
        help="Number of timesteps in each sample"
    )

    args = parser.parse_args()
    
    features_path = args.features_path
    target_path = args.target_path
    output_path = args.output_path
    lat_dim = args.lat_dim
    lon_dim = args.lon_dim
    max_gap = args.max_gap
    timestep = args.timestep

    # ===================
    # Features processing
    # ===================

    # Creating an empty dataset
    combined_ds = xr.Dataset(
        {
            "data": (("time", "lat", "lon", "channel"), np.empty((0, lat_dim, lon_dim, 0))),
        },
        coords={
            "lat": np.arange(lat_dim),
            "lon": np.arange(lon_dim),
            "channel": [],
        },
        attrs={
            "description": "Consolidated NetCDF file with multiple features",
        },
    )

    # Process each feature
    for feature_name in sorted(glob(f"{features_path}/*")):
        print('Processing', feature_name)
        if os.path.isdir(feature_name):
            feature_name_short = os.path.basename(feature_name)
            file_pattern = f"{feature_name}/**/*.nc"
            process_feature(feature_name_short, file_pattern)
    
    # =================
    # Target processing
    # =================

    # Creating another empty dataset
    target_ds = xr.Dataset(
        {
            "data": (("time", "lat", "lon", "channel"), np.empty((0, lat_dim, lon_dim, 0))),
        },
        coords={
            "lat": np.arange(lat_dim),
            "lon": np.arange(lon_dim),
            "channel": [],
        },
        attrs={
            "description": "Consolidated NetCDF file with multiple features",
        },
    )

    # Process each target
    for target_name in sorted(glob(f"{target_path}/*")):
        print('Processing', target_name)
        if os.path.isdir(target_name):
            target_name_short = os.path.basename(target_name)
            file_pattern = f"{target_name}/**/*.nc"
            process_target(target_name_short, file_pattern)

    # =================
    print("Collecting samples...")
    X_samples, Y_samples = collect_samples(combined_ds, target_ds, timestep, max_gap)

    # Convert X_samples and Y_samples to numpy arrays
    X_array = np.stack(X_samples)
    Y_array = np.stack(Y_samples)

    # Pad Y_array along the channel dimension to match the number of channels in X_array
    required_channels = X_array.shape[-1]  # e.g., 5
    current_channels = Y_array.shape[-1]   # e.g., 1

    if current_channels < required_channels:
        pad_amount = required_channels - current_channels
        # np.pad expects a pad_width tuple for each dimension: (before, after)
        Y_array = np.pad(Y_array, pad_width=((0, 0),  # sample dimension
                                             (0, 0),  # time dimension
                                             (0, 0),  # lat dimension
                                             (0, 0),  # lon dimension
                                             (0, pad_amount)),  # channel dimension
                        mode='constant', constant_values=0)
        
    # Extract coordinates from the dataset
    lat = combined_ds.lat.values
    lon = combined_ds.lon.values

    # Cria o xarray.Dataset
    output_ds = xr.Dataset(
        {
            "x": (["sample", "time", "lat", "lon", "channel"], X_array),
            "y": (["sample", "time", "lat", "lon", "channel"], Y_array),
        },
        coords={
            "lat": lat,
            "lon": lon,
        },
        attrs={
            "description": "The variables have weather features values and are separable in x and y, "
                            "which are to be used as input and target of the machine learning algorithms, respectively."
        },
    )


    # Saves the dataset into a NetCDF file
    output_ds.to_netcdf(output_path)
    print("Dataset salvo em", output_path)
