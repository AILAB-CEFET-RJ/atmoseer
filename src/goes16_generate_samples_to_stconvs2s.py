import argparse
from glob import glob
import os
import re
import numpy as np
import pandas as pd
import xarray as xr


def process_feature(feature_name, file_pattern):
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
        # Sort timestamps and data_list together by timestamp
        sorted_pairs = sorted(zip(timestamps, data_list))
        timestamps_sorted, data_list_sorted = zip(*sorted_pairs)
        timestamps = list(timestamps_sorted)
        data_list = list(data_list_sorted)

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

        return feature_ds
    return None

def process_target(target_name, file_pattern):
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
        # Sort timestamps and data_list together by timestamp
        sorted_pairs = sorted(zip(timestamps, data_list))
        timestamps_sorted, data_list_sorted = zip(*sorted_pairs)
        timestamps = list(timestamps_sorted)
        data_list = list(data_list_sorted)

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

        return feature_ds
    return None


# Function to check for maximum gap within a sample
def check_max_gap(timestamps):
    """Check the maximum time difference (gap) in minutes within a sample."""
    time_deltas = timestamps.diff(dim="time") / np.timedelta64(1, "m")  # Convert to minutes
    max_gap = time_deltas.max().item() if len(time_deltas) > 0 else 0
    return max_gap


# Function to collect samples
def collect_samples(features_ds, target_ds, timestep, max_gap, offset):
    """Collect valid X_samples and Y_samples from features_ds and target_ds."""
    X_samples, Y_samples, accepted_indices = [], [], []
    times = features_ds.time  # Keep as xarray.DataArray to use diff()
    expected_x_shape = None
    expected_y_shape = None
    # Iterate through the features_ds in sliding windows
    for i in range(len(times) - (timestep + offset)):
        time_window_x = times.isel(time=slice(i, i + timestep))
        time_window_y = times.isel(time=slice(i + offset, i + offset + timestep))

        # Check for gaps in X_sample's time window
        if check_max_gap(time_window_x) > max_gap:
            continue  # Skip samples with large gaps
        if check_max_gap(time_window_y) > max_gap:
            continue  # Skip samples with large gaps
        # Extract X and Y samples
        X_sample = features_ds.isel(time=slice(i, i + timestep)).data
        Y_sample = target_ds.isel(time=slice(i + offset, i + offset + timestep)).data

        # Set expected shapes on first iteration
        if expected_x_shape is None:
            expected_x_shape = X_sample.shape
        if expected_y_shape is None:
            expected_y_shape = Y_sample.shape

        # Pad X_sample if needed
        if X_sample.shape != expected_x_shape:
            pad_width = [(0, max(0, expected_x_shape[j] - X_sample.shape[j])) for j in range(len(expected_x_shape))]
            X_sample = np.pad(X_sample, pad_width, mode='constant', constant_values=0)
        # Pad Y_sample if needed
        if Y_sample.shape != expected_y_shape:
            pad_width = [(0, max(0, expected_y_shape[j] - Y_sample.shape[j])) for j in range(len(expected_y_shape))]
            Y_sample = np.pad(Y_sample, pad_width, mode='constant', constant_values=0)

        # Only append if shapes now match expected
        if X_sample.shape == expected_x_shape and Y_sample.shape == expected_y_shape:
            X_samples.append(X_sample)
            Y_samples.append(Y_sample)
            accepted_indices.append(i)
        else:
            print(f"Skipping sample at index {i} due to shape mismatch after padding: X {X_sample.shape}, Y {Y_sample.shape}")
    return X_samples, Y_samples, accepted_indices


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
    offset = args.timestep

    # ===================
    # Features processing
    # ===================
    feature_datasets = []
    feature_times = None
    for feature_name in sorted(glob(f"{features_path}/*")):
        print('Processing', feature_name)
        if os.path.isdir(feature_name):
            feature_name_short = os.path.basename(feature_name)
            file_pattern = f"{feature_name}/**/*.nc"
            ds = process_feature(feature_name_short, file_pattern)
            if ds is not None:
                feature_datasets.append(ds)
                if feature_times is None:
                    feature_times = set(ds.time.values)
                else:
                    feature_times = feature_times & set(ds.time.values)
    if not feature_datasets:
        raise RuntimeError("No feature datasets found!")
    # Intersect all feature times
    feature_times = sorted(feature_times)
    # Subset all features to common times
    feature_datasets = [ds.sel(time=feature_times) for ds in feature_datasets]
    # Concatenate along channel
    combined_ds = xr.concat(feature_datasets, dim="channel")

    # =================
    # Target processing
    # =================
    target_datasets = []
    target_times = None
    for target_name in sorted(glob(f"{target_path}/*")):
        print('Processing', target_name)
        if os.path.isdir(target_name):
            target_name_short = os.path.basename(target_name)
            file_pattern = f"{target_name}/**/*.nc"
            ds = process_target(target_name_short, file_pattern)
            if ds is not None:
                target_datasets.append(ds)
                if target_times is None:
                    target_times = set(ds.time.values)
                else:
                    target_times = target_times & set(ds.time.values)
    if not target_datasets:
        raise RuntimeError("No target datasets found!")
    # Intersect all target times
    target_times = sorted(target_times)
    # Subset all targets to common times
    target_datasets = [ds.sel(time=target_times) for ds in target_datasets]
    # Concatenate along channel
    target_ds = xr.concat(target_datasets, dim="channel")

    # ===============
    # Align features and targets on common times
    # ===============
    common_times = sorted(set(combined_ds.time.values) & set(target_ds.time.values))
    combined_ds = combined_ds.sel(time=common_times)
    target_ds = target_ds.sel(time=common_times)

    print("Collecting samples...")
    X_samples, Y_samples, accepted_indices = collect_samples(combined_ds, target_ds, timestep, max_gap, offset)

    # Convert X_samples and Y_samples to numpy arrays
    X_array = np.stack(X_samples)
    Y_array = np.stack(Y_samples)

    # STConvS2S requires input and output to have same number of channels
    # Replicate precipitation data across all 5 channels to match X_array
    required_channels = X_array.shape[-1]  # Should be 5
    current_channels = Y_array.shape[-1]   # Should be 1
    
    if current_channels == 1 and required_channels > 1:
        # Replicate precipitation across all channels
        Y_array = np.repeat(Y_array, required_channels, axis=-1)
        print(f"Replicated precipitation data across {required_channels} channels to match X input structure")
    
    print(f"X_array shape: {X_array.shape}")
    print(f"Y_array shape: {Y_array.shape}")
    print("STConvS2S requires matching channel dimensions between input and output")

    lat = combined_ds.lat.values
    lon = combined_ds.lon.values

    # Extract feature names from the combined dataset
    feature_names = combined_ds.channel.values.tolist()
    print(f"Feature names: {feature_names}")


    # Extract sample timestamps for X and Y - for each accepted sample, get the timestamps of that sample
    sample_x_timestamps = []
    sample_y_timestamps = []
    times_array = combined_ds.time.values  # Get the actual datetime timestamps from the dataset
    for i in accepted_indices:
        # X_sample uses time slice [i:i+timestep]
        sample_x_timestamps.append(times_array[i:i+timestep])
        # Y_sample uses time slice [i+offset:i+offset+timestep]
        sample_y_timestamps.append(times_array[i+offset:i+offset+timestep])

    sample_x_timestamps_array = np.array(sample_x_timestamps)
    sample_y_timestamps_array = np.array(sample_y_timestamps)
    print(f"Sample X timestamps shape: {sample_x_timestamps_array.shape}")
    print(f"Sample Y timestamps shape: {sample_y_timestamps_array.shape}")
    print(f"First sample X timestamps: {sample_x_timestamps_array[0] if len(sample_x_timestamps_array) > 0 else 'No samples'}")
    print(f"First sample Y timestamps: {sample_y_timestamps_array[0] if len(sample_y_timestamps_array) > 0 else 'No samples'}")

    output_ds = xr.Dataset(
        {
            "x": (["sample", "time", "lat", "lon", "channel"], X_array),
            "y": (["sample", "time", "lat", "lon", "channel"], Y_array),
        },
        coords={
            "sample": np.arange(len(X_samples)),
            "time": np.arange(timestep),  # Relative time indices within each sample
            "lat": lat,
            "lon": lon,
            "channel": feature_names,  # Add feature names as channel coordinates
            "sample_x_timestamps": (["sample", "time"], sample_x_timestamps_array),  # Actual timestamps for each X sample
            "sample_y_timestamps": (["sample", "time"], sample_y_timestamps_array),  # Actual timestamps for each Y sample
        },
        attrs={
            "description": "X contains 9 meteorological feature channels, Y contains precipitation data replicated across 9 channels. "
                            "X and Y have matching channel dimensions as required by STConvS2S architecture.",
            "feature_channels": str({
                f"channel_{i}": feature_names[i] for i in range(len(feature_names))
            }),
            "timestep": timestep,
            "max_gap_minutes": max_gap,
            "total_samples": len(X_samples),
            "creation_date": str(np.datetime64('now')),
        },
    )

    # No fillna(0) needed if alignment is correct
    output_ds.to_netcdf(output_path)
    print("Dataset salvo em", output_path)
