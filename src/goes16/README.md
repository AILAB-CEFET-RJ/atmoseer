## GOES-16 ABI Downloader and Cropper

The script `goes16_download_crop.py` allows you to:

- Download GOES-16 ABI data from AWS (`noaa-goes16` S3 bucket)
- Select a specific ABI channel (e.g., C08)
- Define a date range for download
- Crop and resample data to a specific geographic bounding box
- Save one NetCDF file per day of imagery

### Usage with Makefile

You can execute the downloader/cropper using the provided `Makefile`. An example:

```bash
make goes16-download-crop START=2024-04-14 END=2024-01-01 CHANNEL=08 DIR=./data/goes16/CMI/2024/C08
```

This will create daily NetCDF files (reprojected and cropped) in:

```
data/goes16/CMI/2024/C08/
```

Make sure to set up AWS CLI credentials and install dependencies like `s3fs`, `xarray`, and `pyproj`.

---

## GOES-16 Feature Extractor

This module provides feature engineering routines to extract physically meaningful variables from GOES-16 ABI satellite imagery. These features can be used to support precipitation nowcasting models.

### Location

All scripts are contained in:

```
src/goes16/
├── features/                   # This folder contains one function per feature extractor
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

### How to Run

Use the Makefile to extract selected features. An example:

```bash
make goes16-features FEATS="--pn --gtn --fa --wv_grad --li_proxy --toct --pn_std --verbose"
```


### Expected Input Structure

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

### Output

Features are saved to a file following the format below:

```
features/CMI/{feature_name}/{year}/FEATURE_TIMESTAMP.nc
```

Where `feature_name` is one of the extracted variables (e.g., `temperatura_topo_nuvem`, `proxy_estabilidade`, etc.).

## Feature Validation and Logging

### Validating Generated Features

You can validate whether the generated features are consistent with the source channels:

```bash
make validate-feature FEAT=pn CANAIS="C09 C13"
```

### Log Analysis

Each feature module generates a `.log` file (e.g. pn.log, gtn.log, ...) in `src/goes16/features/`.

To summarize warnings and errors across all logs:

```bash
make analyze-logs
```