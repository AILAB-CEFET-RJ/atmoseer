## 🌧️ GPM Preprocessing Scripts

This module contains scripts to process GPM (IMERG) precipitation data:

- `gpm_download_crop.py`: Download and spatially crop GPM data to a given bounding box
- `gpm_plot.py`: Visualize GPM precipitation fields

---

## 🛠️ Usage via Makefile

You can call the GPM downloader with cropping directly from the root of the repository using:

make gpm-download-crop BEGIN=2024/01/01 END=2024/01/03 USER=your_username PWD=your_password

### 📌 Optional Flags

You can pass optional arguments like:

- DEBUG=--debug → Enables verbose mode.
- IGNORED="--ignored_months 6 7" → Skips specified months.

### 🧩 Example with All Options

make gpm-download-crop \
  BEGIN=2024/01/01 \
  END=2024/01/31 \
  USER=your_username \
  PWD=your_password \
  DEBUG=--debug \
  IGNORED="--ignored_months 6 7"

> Make sure your NASA Earthdata credentials are valid and authorized to access IMERG files.
