"""
Copyright 2022 ARC Centre of Excellence for Climate Systems Science

author: Paola Petrelli <paola.petrelli@utas.edu.au>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

 This script is used to download and/or update the GPM-IMERG V06 dataset on
   the NCI server from https://disc.gsfc.nasa.gov/datasets/GPM_3IMERGHH_06/summary
 Last change:
      2022-07-19

 Usage:
 Inputs are:
   y - year to check/download/update the only one required
   f - this forces local chksum to be re-calculated even if local file exists
 The script will look for the local and remote checksum files:
     trmm_<local/remote>_cksum_<year>.txt
 If the local file does not exists calls calculate_cksum() to create one
 If the remote cksum file does not exist calls retrieve_cksum() to create one
 The remote checksum are retrieved directly from the cksum field in
   the filename.xml available online.
 The checksums are compared for each files and if they are not matching
   the local file is deleted and download it again using the requests module
 The requests module also handle the website cookies by opening a session
   at the start of the script

 Uses the following modules:
 import requests to download files and html via http
 import beautifulsoup4 to parse html
 import xml.etree.cElementTree to read a single xml field
 import time and calendar to convert timestamp in filename
        to day number from 1-366 for each year
 import subprocess to run cksum as a shell command
 import argparse to manage inputs
 should work with both python 2 and 3

"""

import argparse

# try:
#     import xml.etree.cElementTree as ET
# except ImportError as e:
#     print(f"Pass a password as input or set the GPMPWD variable: {e}")
# from util import set_log, check_mdt, print_summary
import logging
import os
import re
import sys
import time
from datetime import datetime, timedelta

import dateutil.parser
import numpy as np
import pytz
import requests
from bs4 import BeautifulSoup
from netCDF4 import Dataset

from config import globals


def set_log(name, fname, level):
    """Set up logging with a file handler

    Parameters
    ----------
    name: str
        Name of logger object
    fname: str
         Log output filename
    level: str
        Base logging level
    """

    # First disable default root logger
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    # start a logger
    logger = logging.getLogger(name)
    # set a formatter to manage the output format of our handler
    logging.Formatter("%(asctime)s | %(message)s", "%H:%M:%S")
    minimal = logging.Formatter("%(message)s")
    if level == "debug":
        minimal = logging.Formatter("%(levelname)s: %(message)s")
    # set the level passed as input, has to be logging.LEVEL not a string
    log_level = logging.getLevelName(level.upper())
    logger.setLevel(log_level)
    # add a handler for console this will have the chosen level
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(minimal)
    logger.addHandler(console_handler)
    # add a handler for the log file, this is set to INFO level
    file_handler = logging.FileHandler(fname)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(minimal)
    logger.addHandler(file_handler)
    # return the logger object
    logger.propagate = False
    return logger


def check_mdt(
    req, fpath, logger, remoteModDate=None, furl=None, head_key="Last-modified"
):
    """Check local and remote modified time and return comparison
    You have to pass either the remote last modified date or
    the file url to try to retrieve it
    """
    if not remoteModDate:
        response = req.head(furl)
        remoteModDate = response.headers[head_key]
    remoteModDate = dateutil.parser.parse(remoteModDate)
    localModDate = datetime.fromtimestamp(os.path.getmtime(fpath))
    localModDate = localModDate.replace(tzinfo=pytz.UTC)
    to_update = localModDate < remoteModDate
    logger.debug(f"File: {fpath}")
    logger.debug(f"Local mod_date: {localModDate}")
    logger.debug(f"ftp mod_date: {remoteModDate}")
    logger.debug(f"to update: {to_update}")
    return to_update


def print_summary(updated, new, error, logger):
    """Print a summary of new, updated and error files to log file"""

    logger.info("==========================================")
    logger.info("Summary")
    logger.info("==========================================")
    logger.info("These files were updated: ")
    for f in updated:
        logger.info(f"{f}")
    logger.info("==========================================")
    logger.info("These are new files: ")
    for f in new:
        logger.info(f"{f}")
    logger.info("==========================================")
    logger.info("These files and problems: ")
    for f in error:
        logger.info(f"{f}")
    logger.info("\n\n")


def parse_input():
    """Parse input arguments"""
    parser = argparse.ArgumentParser(
        description="""Retrieve GPM-IMERG data from NASA server for a given date range.""",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    parser.add_argument(
        "--ignored_months",
        type=int,
        nargs="*",
        default=[],
        help="List of month numbers (1-12) to ignore in the download process. Example: --ignored_months 6 7",
    )

    parser.add_argument(
        "--begin_date", type=str, required=True, help="Start date in format YYYY/MM/DD"
    )
    parser.add_argument(
        "--end_date", type=str, required=True, help="End date in format YYYY/MM/DD"
    )
    parser.add_argument("-u", "--user", type=str, required=True, help="User account")
    parser.add_argument(
        "-p",
        "--pwd",
        type=str,
        default=None,
        required=False,
        help="Account password (optional if GPMPWD env variable is set)",
    )
    parser.add_argument(
        "-d",
        "--debug",
        action="store_true",
        required=False,
        help="Print out debug information (default is False)",
    )

    return vars(parser.parse_args())


def delete_file(file_path):
    try:
        os.remove(file_path)
        print(f"File '{file_path}' successfully removed.")
    except FileNotFoundError:
        print(f"File '{file_path}' not found.")
    except PermissionError:
        print(f"No permission to delete the file '{file_path}'.")
    except Exception as e:
        print(f"Error deleting file: {e}")


def robust_download(session, url, retries=5, backoff_factor=2, **kwargs):
    """
    Download a file with retries and exponential backoff.
    Args:
        session (requests.Session): The requests session to use for the download.
        url (str): The URL of the file to download.
        retries (int): Number of retry attempts.
        backoff_factor (int): Factor by which to increase wait time between retries.
        **kwargs: Additional keyword arguments to pass to requests.get().
    Returns:
        requests.Response: The response object if the download is successful.
        None: If the download fails after all retries.
    """
    for attempt in range(retries):
        try:
            response = session.get(url, timeout=600, **kwargs)
            response.raise_for_status()
            return response
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            wait_time = backoff_factor**attempt
            print(
                f"Attempt {attempt + 1} failed: {e}. Retrying in {wait_time} seconds..."
            )
            time.sleep(wait_time)
        except requests.exceptions.RequestException as e:
            print(f"Permanent failure: {e}")
            break
    return None


def download_file(session, url, fname, local_dir, size, data_log):
    """Download file using requests"""
    status = "fine"
    data_log.debug(url)
    try:
        # Optionally, set a timeout for the request to prevent hanging.
        r = robust_download(session, url)
        if r is None:
            print(f"Failed to download {url} after multiple retries.")
            sys.exit(1)
        r.raise_for_status()  # Optionally raise HTTPError for bad responses (4xx, 5xx)
    except requests.exceptions.Timeout as e:
        print("The request timed out:", e)
        # Handle timeout, e.g., retry or log the error
        sys.exit(1)
    except requests.exceptions.ConnectionError as e:
        print("A connection error occurred:", e)
        # Handle connection error
        sys.exit(1)
    except requests.exceptions.HTTPError as e:
        print("An HTTP error occurred:", e)
        # Handle HTTP errors
        sys.exit(1)
    except requests.exceptions.RequestException as e:
        print("An error occurred while making the request:", e)
        # Handle any other type of request exception
        sys.exit(1)

    with open(fname, "wb") as f:
        f.write(r.content)
    del r

    local_size = int(os.stat(fname).st_size)
    if local_size < size:
        status = "error"

    try:
        timestamp, precipitation = crop_precipitation(fname)

        var_name = timestamp.strftime("%Y_%m_%d_%H_%M")
        day_str = timestamp.strftime("%Y-%m-%d")

        file_path = f"{local_dir}/{day_str}.nc"

        update_or_create_netcdf(file_path, var_name, precipitation)
        delete_file(fname)

    except Exception as e:
        status = "error"
        print(f"Error cropping file: {e}")

    return status


def update_or_create_netcdf(file_path, var_name, var_value):
    """
    Atualiza ou cria um arquivo NetCDF.

    Args:
    - file_path: Caminho do arquivo NetCDF.
    - var_name: Nome da variável a ser adicionada.
    - var_value: Valor da nova variável.
    - dimensions: Dimensões para a variável (default: ('time',)).
    """

    # Verifica se o arquivo já existe
    if os.path.exists(file_path):
        print("Arquivo existe. Atualizando...")
        # Abrir no modo append
        with Dataset(file_path, mode="a") as dataset:
            if var_name not in dataset.variables:
                # Create dimensions based on the shape of the numpy array
                for i, dim_size in enumerate(var_value.shape):
                    dim_name = f"dim_{i}_{var_name}"
                    if dim_name not in dataset.dimensions:
                        dataset.createDimension(dim_name, dim_size)

                # Create a variable with the timestamp as its name
                var = dataset.createVariable(
                    var_name,
                    var_value.dtype,
                    tuple(f"dim_{i}_{var_name}" for i in range(var_value.ndim)),
                )

                # Assign the data from the numpy array to the variable
                var[:] = var_value

            else:
                print(f"A variável '{var_name}' já existe.")
    else:
        print("Arquivo não existe. Criando...")
        # Criar novo arquivo NetCDF
        with Dataset(file_path, mode="w") as nc:
            # Create dimensions based on the shape of the numpy array
            for i, dim_size in enumerate(var_value.shape):
                dim_name = f"dim_{i}_{var_name}"
                if dim_name not in nc.dimensions:
                    nc.createDimension(dim_name, dim_size)

            # Create a variable with the timestamp as its name
            var = nc.createVariable(
                var_name,
                var_value.dtype,
                tuple(f"dim_{i}_{var_name}" for i in range(var_value.ndim)),
            )

            # Assign the data from the numpy array to the variable
            var[:] = var_value


def crop_precipitation(fname):
    dataset = Dataset(fname, "r", format="NETCDF4")

    header = dataset.getncattr("FileHeader").split(";")

    for elem in header:
        if "StartGranuleDateTime" in elem:
            timestamp_str = elem.split("=")[1]
            timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))

    # Abrindo o arquivo
    lat = dataset.variables["lat"][:]
    lon = dataset.variables["lon"][:]
    precipitation = dataset.variables["precipitation"]

    # Define the latitude bounds
    lower_latitude_bound = globals.lat_min
    upper_latitude_bound = globals.lat_max

    # Define the longitude bounds
    lower_longitude_bound = globals.lon_min
    upper_longitude_bound = globals.lon_max

    # Find latitude indices of elements within the bounds
    indices_in_range_latitude = np.where(
        (lat >= lower_latitude_bound) & (lat <= upper_latitude_bound)
    )[0]
    indices_in_range_longitude = np.where(
        (lon >= lower_longitude_bound) & (lon <= upper_longitude_bound)
    )[0]
    precipitation_data = precipitation[0, indices_in_range_longitude, :][
        :, indices_in_range_latitude
    ].T

    dataset.close()  # Feche o arquivo para liberar o bloqueio

    return timestamp, precipitation_data


def download_yr(session, http_url, yr, data_dir, days, data_log):
    """Download the whole year directory"""
    r = session.get(f"{http_url}/{yr}/contents.html")
    soup = BeautifulSoup(r.content, "html.parser")
    status = {"new": [], "updated": [], "error": []}
    # find all links with 3 digits indicating day of year folders
    for link in soup.find_all("a", string=re.compile("^\d{3}/")):
        subdir = link.get("href").rstrip()
        if days != [] and subdir[:3] not in days:
            data_log.debug(f"skipping {subdir[:3]}")
            continue
        r2 = session.get(f"{http_url}/{yr}/{subdir}")
        soup2 = BeautifulSoup(r2.content, "html.parser")
        # the same href file link is repeated in the html,
        # so we need to keep track of what we already checked
        done_list = []
        for sub in soup2.find_all("a", href=re.compile("^3B-HHR.*\.HDF5\..*\.html$")):
            href = sub.get("href")
            if sub.find_next("time"):
                sub_next = sub.find_next("time")
                last_mod = sub_next.text.strip()
                size = sub_next.find_next("td").text.strip()
                data_log.debug(f"{href}: {last_mod}, {size}")
            if href in done_list:
                continue
            else:
                done_list.append(href)
                status = process_file(
                    session,
                    data_dir,
                    yr,
                    http_url,
                    subdir,
                    href,
                    last_mod,
                    int(size),
                    status,
                    data_log,
                )
                if status == "error":
                    print(f"Downloading failed: {http_url}/{subdir}/{href}")
    data_log.info(f"Download for year {yr} is complete")
    return status


def process_file(
    session, data_dir, yr, http_url, subdir, href, last_mod, size, status, data_log
):
    """Check if file exists and/or needs updating, if new or to update,
    download file
    """
    fname = href.replace("HDF5.dmr.html", "nc")
    local_name = f"{data_dir}/{yr}/{fname}"
    local_dir = f"{data_dir}/{yr}"
    if not os.path.exists(local_name):
        data_log.debug(f"New file: {local_name}")
        furl = (
            f"{http_url}/{yr}/"
            + f"{subdir.replace('contents.html', '')}"
            + f"{href.replace('.dmr.html', '.dap.nc4')}"
        )
        data_log.debug(furl)
        st = download_file(session, furl, local_name, local_dir, size, data_log)
        if st == "error":
            status["error"].append(local_name)
        else:
            status["new"].append(local_name)
    else:
        update = check_mdt(session, local_name, data_log, remoteModDate=last_mod)
        if update:
            os.remove(local_name)
            st = download_file(session, furl, local_name, local_dir, size, data_log)
            if st == "error":
                status["error"].append(local_name)
            else:
                status["updated"].append(local_name)
    return status


def open_session(usr, pwd):
    """Open a requests session to manage connection to server"""
    session = requests.session()
    p = session.post("http://urs.earthdata.nasa.gov", {"user": usr, "password": pwd})
    print(f"session.post: {p}")
    requests.utils.dict_from_cookiejar(session.cookies)
    return session


def main():
    """
    Before running this script, perform the three steps described below.

    1) Create an user account in the Earthdata Portal (https://urs.earthdata.nasa.gov/users/new)

    2) Configure your username and password for authentication using a .netrc file
    > cd ~
    > touch .netrc
    > echo "machine urs.earthdata.nasa.gov login uid_goes_here password password_goes_here" > .netrc
    > chmod 0600 .netrc
    where uid_goes_here is your Earthdata Login username and password_goes_here is your Earthdata Login password.
    Note that some password characters can cause problems. A backslash or space anywhere in your password will need to be escaped with an additional backslash.
    Similarly, if you use a '#' as the first character of your password, it will also need to be escaped with a preceding backslash.
    Depending on your environment, the use of double-quotes " may be turned into "smart-quotes" automatically. We recommend turning this feature off.
    Some users have found that the double quotes are not supported by their systems.
    Some users have found that the > is aliased to >> on some machines.
    This will append the text instead of overwrite the text.
    We recommend checking your ~/.netrc file to ensure it only has one line.

    3) Create a cookie file.
    This will be used to persist sessions across individual cURL/Wget calls, making it more efficient.
    > cd ~
    > touch .urs_cookies
    """
    args = parse_input()

    begin_date = datetime.strptime(args["begin_date"], "%Y/%m/%d")
    end_date = datetime.strptime(args["end_date"], "%Y/%m/%d")
    user = args["user"]

    if begin_date > end_date:
        raise ValueError("begin_date must be earlier than or equal to end_date")

    if begin_date.year != end_date.year:
        raise ValueError("Date range must be within the same year")

    yr = str(begin_date.year)
    ignored_months = args.get("ignored_months", [])

    # Gerar lista de dias julianos válidos
    delta = end_date - begin_date
    days = []
    for i in range(delta.days + 1):
        current_date = begin_date + timedelta(days=i)
        if current_date.month not in ignored_months:
            julian_day = str(current_date.timetuple().tm_yday).zfill(3)
            days.append(julian_day)

    try:
        pwd = args["pwd"]
        if pwd is None:
            pwd = os.getenv("GPMPWD")
    except KeyError as e:
        print(f"Pass a password as input or set the GPMPWD variable: {e}")

    today = datetime.today().strftime("%Y-%m-%d")
    sys_user = os.getenv("USER")
    root_dir = os.getenv("AUSREFDIR", ".")
    run_dir = f"{root_dir}"

    http_url = (
        "https://gpm1.gesdisc.eosdis.nasa.gov/opendap/hyrax/GPM_L3/GPM_3IMERGHH.07"
    )
    data_dir = f"{root_dir}/data/GPM"
    flog = f"{run_dir}/gpm_update_log.txt"

    level = "info"
    if args.get("debug", False):
        level = "debug"
    data_log = set_log("gpmlog", flog, level)

    directory = f"{data_dir}/{yr}"
    if not os.path.exists(directory):
        os.mkdir(directory)
        print(f"Directory '{directory}' created.")
    else:
        print(f"Directory '{directory}' already exists.")

    session = open_session(user, pwd)
    status = download_yr(session, http_url, yr, data_dir, days, data_log)

    data_log.info(f"Updated on {today} by {sys_user}")
    print_summary(status["updated"], status["new"], status["error"], data_log)


if __name__ == "__main__":
    main()
