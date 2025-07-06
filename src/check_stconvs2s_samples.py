import xarray as xr
import numpy as np
import sys

# Usage: python check_stconvs2s_samples.py <path_to_output_dataset.nc>
if len(sys.argv) < 2:
    print("Usage: python check_stconvs2s_samples.py <path_to_output_dataset.nc>")
    sys.exit(1)

output_path = sys.argv[1]
ds = xr.open_dataset(output_path)

print("\n===== STConvS2S Sample Consistency Check =====\n")

# 1. Check variable existence
for var in ["x", "y"]:
    if var not in ds:
        print(f"ERROR: Variable '{var}' not found in dataset!")
        sys.exit(1)
    else:
        print(f"Variable '{var}' found.")

# 2. Check shapes and dtypes
x_shape = ds['x'].shape
y_shape = ds['y'].shape
print(f"x shape: {x_shape}, dtype: {ds['x'].dtype}")
print(f"y shape: {y_shape}, dtype: {ds['y'].dtype}")

if x_shape[0] != y_shape[0]:
    print(f"WARNING: Number of samples (first dim) differs: x={x_shape[0]}, y={y_shape[0]}")
if x_shape[1] != y_shape[1]:
    print(f"WARNING: Number of time steps differs: x={x_shape[1]}, y={y_shape[1]}")
if x_shape[2] != y_shape[2] or x_shape[3] != y_shape[3]:
    print(f"WARNING: Spatial dimensions differ between x and y!")

# 3. Check for NaNs or infs
for var in ["x", "y"]:
    arr = ds[var].values
    n_nan = np.isnan(arr).sum()
    n_inf = np.isinf(arr).sum()
    print(f"{var}: {n_nan} NaNs, {n_inf} infs")
    if n_nan > 0 or n_inf > 0:
        print(f"WARNING: {var} contains NaNs or infs!")

# 4. Check for all-zero, all-NaN, and constant samples

def check_all_zero_samples(arr, varname):
    all_zero = np.all(arr == 0, axis=(1,2,3,4))
    n_all_zero = np.sum(all_zero)
    if n_all_zero > 0:
        print(f"WARNING: {n_all_zero} all-zero samples found in {varname} (possible padding or missing data)")
    else:
        print(f"No all-zero samples in {varname}.")
    return all_zero

def check_all_nan_samples(arr, varname):
    all_nan = np.all(np.isnan(arr), axis=(1,2,3,4))
    n_all_nan = np.sum(all_nan)
    if n_all_nan > 0:
        print(f"WARNING: {n_all_nan} all-NaN samples found in {varname}!")
    else:
        print(f"No all-NaN samples in {varname}.")
    return all_nan

def check_constant_samples(arr, varname):
    # Check if all values in a sample are the same (not just zero)
    flat = arr.reshape(arr.shape[0], -1)
    is_const = np.all(flat == flat[:, [0]], axis=1)
    n_const = np.sum(is_const)
    if n_const > 0:
        print(f"WARNING: {n_const} constant-value samples found in {varname} (all values identical)")
    else:
        print(f"No constant-value samples in {varname}.")
    return is_const

for var in ["x", "y"]:
    arr = ds[var].values
    check_all_zero_samples(arr, var)
    check_all_nan_samples(arr, var)
    check_constant_samples(arr, var)

# 5. Print summary statistics and unique values
print("\nSummary statistics:")
for var in ["x", "y"]:
    arr = ds[var].values
    print(f"{var}: min={arr.min()}, max={arr.max()}, mean={arr.mean()}, std={arr.std()}")
    unique_vals = np.unique(arr)
    print(f"{var}: unique values (up to 10 shown): {unique_vals[:10]}")
    print(f"{var}: number of unique values: {len(unique_vals)}")
    # Check for negative values
    if (arr < 0).any():
        print(f"WARNING: {var} contains negative values!")
    # Print first and last few values for a quick glance
    print(f"{var}: first 5 values: {arr.flatten()[:5]}")
    print(f"{var}: last 5 values: {arr.flatten()[-5:]}")

# 6. Print a few random sample slices for inspection
np.random.seed(42)
for var in ["x", "y"]:
    arr = ds[var].values
    n_samples = arr.shape[0]
    idxs = np.random.choice(n_samples, min(2, n_samples), replace=False)
    print(f"\n{var}: Showing slices for random samples: {idxs}")
    for idx in idxs:
        print(f"Sample {idx} (all timesteps, first channel):\n{arr[idx,:,0,:,:]}")

# 7. Sliding window health checks
if "sample" in ds.dims and "time" in ds.dims:
    print("\nSliding window health checks:")
    n_samples = ds.dims["sample"]
    n_time = ds.dims["time"]
    # Check for duplicate samples (by hash)
    x_flat = ds["x"].values.reshape(n_samples, -1)
    y_flat = ds["y"].values.reshape(n_samples, -1)
    x_hashes = [hash(arr.tobytes()) for arr in x_flat]
    y_hashes = [hash(arr.tobytes()) for arr in y_flat]
    n_x_dupes = len(x_hashes) - len(set(x_hashes))
    n_y_dupes = len(y_hashes) - len(set(y_hashes))
    print(f"Duplicate x samples: {n_x_dupes}")
    print(f"Duplicate y samples: {n_y_dupes}")
    # Print time coverage if time coordinate is present
    if "time" in ds.coords:
        times = ds["time"].values
        print(f"Time coverage: {times[0]} to {times[-1]}")
        print(f"Number of unique time points: {len(np.unique(times))}")
    # Check for consecutive time steps in a few random samples
    if "time" in ds.coords:
        np.random.seed(123)
        idxs = np.random.choice(n_samples, min(3, n_samples), replace=False)
        for idx in idxs:
            sample_times = ds["time"].values[:n_time]  # assumes all samples use same time coord
            time_diffs = np.diff(sample_times).astype('timedelta64[m]').astype(int)
            print(f"Sample {idx} time diffs (minutes): {time_diffs}")

# 8. Check x/y time window alignment for a few samples
if ("sample" in ds.dims and "time" in ds.dims) and ("time" in ds.coords):
    print("\nSliding window x/y time alignment check:")
    n_samples = ds.dims["sample"]
    n_time = ds.dims["time"]
    times = ds["time"].values
    np.random.seed(456)
    idxs = np.random.choice(n_samples, min(3, n_samples), replace=False)
    for idx in idxs:
        # For each sample, print the time window for x and y
        x_times = times[:n_time]
        y_times = times[1:n_time+1]
        print(f"Sample {idx}:")
        print(f"  x times: {[str(t) for t in x_times]}")
        print(f"  y times: {[str(t) for t in y_times]}")
        # Check that y_times is x_times shifted by one
        if np.all(x_times[1:] == y_times[:-1]):
            print("  Alignment: OK (y is x shifted by one)")
        else:
            print("  Alignment: MISALIGNED! (y is not x shifted by one)")

print("\n===== Check complete =====\n")
