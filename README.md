# AtmoSeer

AtmoSeer is a set of Python scripts for downloading, processing, and analyzing atmospheric and meteorological data from sources such as NASA GPM and GOES-16. It supports environment setup via Conda and requires authentication for NASA Earthdata access. This project also provides a pipeline to build rainfall forecast models. The pipeline can be configured with different meteorological data sources.

---

## Installation

### 1. Clone the Repository

```bash
git clone https://github.com/your-org/atmoseer.git
cd atmoseer
````

### 2. Set NASA Earthdata Credentials

Before running the setup script, you must set your NASA Earthdata credentials as environment variables:

```bash
export EARTHDATA_USER="your_username"
export EARTHDATA_PASS="your_password"
```

> You can register for a free NASA Earthdata account at [https://urs.earthdata.nasa.gov](https://urs.earthdata.nasa.gov).

### 3. Run the Setup Script

The `setup.sh` script will:

* Remove any existing Conda environment named `atmoseer`
* Create a new Conda environment using `config/environment.yml`
* Create necessary data directories
* Configure `.netrc` and `.urs_cookies` in your home directory for Earthdata access

```bash
./setup.sh
```

---

## Usage

After the environment is set up, activate it:

```bash
conda activate atmoseer
```

Now, you can start running AtmoSeer scripts.

## Running Scripts with Make

To simplify execution, AtmoSeer supports running preprocessing tasks using `make`. Each data module has its own Makefile targets and README with details.

### ✅ Examples

- **GOES-16 Feature Extraction**

```bash
make goes16-features FEATS="--pn --fa --toct --verbose"
```

See full usage in [`src/goes16/README.md`](src/goes16/README.md)

- **GPM Download and Crop**

```bash
make gpm-download-crop BEGIN=2024/01/01 END=2024/01/03 USER=your_username PWD=your_password
```

See full usage in [`src/gpm/README.md`](src/gpm/README.md)

---

## 📁 Directory Structure

```
atmoseer/
├── config/                 # Conda environment and config files
│   └── environment.yml
├── data/                  # Automatically created data directories
│   ├── datasets/
│   ├── era5/
│   ├── goes16/
    ├── gpm/
│   ├── sounding/
│   ├── surface_stations/
│   └── 
├── src/                   # Scripts for downloading and processing data, and for model training
│   ├── era5/
│   ├── goes16/
    ├── gpm/
│   ├── sounding/
│   ├── surface_stations/
    └── ...
├── setup.sh               # Installation script
└── README.md              # This file
```

---

## 🛠 Requirements

* [Miniconda](https://docs.conda.io/en/latest/miniconda.html) or [Anaconda](https://www.anaconda.com/)
* Linux or WSL (recommended)
* Internet access for installing packages and accessing NASA endpoints

---

## 🧾 License

This project is licensed under the MIT License. See `LICENSE` for details.

---

## 👥 Acknowledgments

* NASA GES DISC — [https://disc.gsfc.nasa.gov](https://disc.gsfc.nasa.gov)
* Earthdata Login Services — [https://urs.earthdata.nasa.gov](https://urs.earthdata.nasa.gov)