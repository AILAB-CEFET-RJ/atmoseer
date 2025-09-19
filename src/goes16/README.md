## 📥 GOES-16 ABI Downloader and Cropper

The script `goes16_download_crop.py` allows you to:

- Download GOES-16 ABI data from AWS (`noaa-goes16` S3 bucket)
- Select a specific ABI channel (e.g., C08)
- Define a date range for download
- Crop and resample data to a specific geographic bounding box
- Save one NetCDF file per day of imagery

### ⚙️ Usage with Makefile

You can execute the downloader/cropper using the provided `Makefile`. An example:

```bash
make goes16-download-crop START=2024-01-01 END=2024-01-01 CHANNEL=08 DIR=./data/goes16/CMI/2024/C08
```

This will create daily NetCDF files (reprojected and cropped) in:

```
data/goes16/CMI/2024/C08/
```

Make sure to set up AWS CLI credentials and install dependencies like `s3fs`, `xarray`, and `pyproj`.

---

## 📦 GOES-16 Feature Extractor

This module provides feature engineering routines to extract physically meaningful variables from GOES-16 ABI satellite imagery. These features can be used to support precipitation nowcasting models.

### 📁 Location

All scripts are contained in:

```
src/goes16/
├── features.py                  # All feature extraction functions
├── main_goes16_features.py     # Command-line script for feature generation
```

### ⚙️ Available Features

| Flag         | Description                                                |
|--------------|------------------------------------------------------------|
| `--pn`       | Cloud depth: difference between channels C09 and C13       |
| `--gtn`      | Glaciation top of clouds: tri-spectral difference (C11,C14,C15) |
| `--fa`       | Temporal derivative of upward flux (based on C13)          |
| `--wv_grad`  | Water vapor gradient: difference C09 - C08                 |
| `--li_proxy` | Stability proxy: difference C14 - C13                      |
| `--toct`     | Cloud Top Temperature (C13 raw)                            |
| `--pn_std`   | Spatial texture (std) of cloud depth (PN)                  |
| `--verbose`  | Print progress messages by year and file count             |

### 🚀 How to Run

Use the Makefile to extract selected features. An example:

```bash
make goes16-features FEATS="--pn --gtn --fa --wv_grad --li_proxy --toct --pn_std --verbose"
```


### 📂 Expected Input Structure

GOES-16 ABI data must be organized as follows:

```
data/
├── goes16/
│   ├── CMI/
│   │   └── <year>/
│   │       ├── C09/
│   │       ├── C13/
│   │       └── ...
│   └── features/
│       └── <feature_name>/
│           └── <year>/
```

Each channel directory contains files named like:

```
C13_2020_01_01_00_00.nc
C13_2020_01_01_00_10.nc
...
```

### 💾 Output

Features are saved to:

```
features/CMI/{feature_name}/{year}/FEATURE_TIMESTAMP.nc
```

Where `feature_name` is one of the extracted variables (e.g., `temperatura_topo_nuvem`, `proxy_estabilidade`, etc.).

## Feature Validation and Logging

### Validating Generated Features

You can validate whether the generated features are consistent with the source channels:

make validate-feature FEAT=pn CANAIS="C09 C13"

### Log Analysis

Each feature module generates a `.log` file (e.g. pn.log, gtn.log, ...) in `src/goes16/features/`.

To summarize warnings and errors across all logs:

make analyze-logs

## Generate Samples for STConvS2S

The script `goes16_generate_samples_to_stconvs2s.py` prepares training samples for the STConvS2S model by combining multiple GOES-16 features and a target variable (e.g., GPM precipitation).

### 📁 Expected Input Structure

Features and target NetCDF files must be organized as follows:

```
features/
  <feature_name>/
    <year>/
      <FEATURE_TIMESTAMP>.nc
target/
  <target_name>/
    <year>/
      <TARGET_TIMESTAMP>.nc
```
Each file contains data for a single day.

### 🚀 How to Run

You can run the sample generator from the command line. Example:

```bash
python goes16_generate_samples_to_stconvs2s.py \
  --features-path ./src/features \
  --target_dir ./src/target \
  --output_file ./src/output_dataset.nc \
  --lat-dim 21 \
  --lon-dim 27 \
  --max-gap 60 \
  --timestep 5
```

**Arguments:**
- `--features_path`: Path to the folder containing all feature subfolders (e.g., ./src/features).
- `--target_path`: Path to the folder containing the target NetCDF files (e.g., ./src/target).
- `--output_path`: Path for the output NetCDF file containing the samples.
- `--lat_dim`: Number of latitude grid points (e.g., 21).
- `--lon_dim`: Number of longitude grid points (e.g., 27).
- `--max_gap`: Maximum allowed gap (in minutes) between consecutive timesteps in a sample.
- `--timestep`: Number of timesteps in each sample

### 📝 What the Script Does

- Loads all selected features and target data, aligning them by timestamp and spatial grid.
- Aggregates data to hourly resolution.
- Collects samples for STConvS2S: each sample contains a window of consecutive timesteps for all features (X) and the target (Y).
- Saves the resulting dataset as a single NetCDF file with dimensions: sample, time, latitude, longitude, and channel.

This output file can be used directly as input of the STConvS2S architecture

## Replacing files in STConvS2S Architecture

The dataset generated by `goes16_generate_samples_to_stconvs2s.py` is designed for direct use with the STConvS2S architecture (see: https://github.com/AILAB-CEFET-RJ/stconvs2s).

**Important:** To ensure compatibility, you must replace several core files in the official STConvS2S repository with the adapted versions from this repository:

- `dataset.py`
- `utils.py`
- `train_evaluate.py`
- `ml_builder.py`
- `loss.py`

These files have been modified to correctly load, preprocess, and handle the NetCDF samples produced by the sample generator script. If you do not replace them, the STConvS2S training pipeline will not work with your new dataset format.

**Steps to update STConvS2S:**
1. Back up the original files in the STConvS2S repository.
2. Copy the above files from the `stconvs2s` folder in this repository.
3. Overwrite the corresponding files in the STConvS2S repository.
4. Follow the standard model training and evaluation workflow as described in the STConvS2S documentation.

**Note:** Always keep backups of the original STConvS2S files in case you need to revert your changes.
=======
## Feature Validation and Logging

## GOES-16 CMI Feature Plotter

This script generates static plots from the NetCDF files of the extracted CMI GOES-16 features.

### ⚙️ Available Features

| Flag         | Feature                                                |
|--------------|------------------------------------------------------------|
| `pn`       | Cloud depth: difference between channels C09 and C13       |
| `pnstd`      | Spatial texture (std) of cloud depth (PN)                  |
| `toct`       | Cloud Top Temperature (C13 raw)                                |
| `c07`  | Particle Size(C07 raw)
| `fa` | Temporal derivative of upward flux (based on C13)                    |
| `li_proxy` |  Stability proxy: difference C14 - C13  |
| `wv_grad`   | Water vapor gradient: difference C09 - C08                 |
| `gtn`   | Glaciation top of clouds: tri-spectral difference (C11,C14,C15) | 

### How to run the plotter via Makefile

The Makefile contains the following rule to run the plotter. Below is an example that generates plots for the Cloud depth feature on May 20, 2023:

```bash
make plot-feature FEAT=pn DATE=2023_05_20
```
After execution, the plots will be saved in `atmoseer/data/goes16/plots/<feature>/<YYYY_MM_DD>/CMI_<YYYY_MM_DD>_<HH>_<MM>.png`.
