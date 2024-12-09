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


try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET
import os
import time, calendar
import argparse
import subprocess
import requests
import re
from bs4 import BeautifulSoup
from datetime import datetime
# from util import set_log, check_mdt, print_summary

import os
import hashlib
import logging
import calendar
from datetime import datetime
from time import gmtime, strptime
import requests
import dateutil.parser
import pytz

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
    formatter = logging.Formatter(
        "%(asctime)s | %(message)s", "%H:%M:%S")
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


# def check_md5sum(filename, logger):
#     """Check local and remote md5 checksum and return comparison
#     This is much slower then checking modified date
#     """

#     m = hashlib.md5()
#     self.ftp.retrbinary('RETR %s' % filename, m.update)
#     ftp_md5 =  m.hexdigest()
#     local_md5 = hashlib.md5(open(filename,'rb').read()).hexdigest()
#     logger.debug(f"File: {filename}")
#     logger.debug(f"Local md5: {local_md5}")
#     logger.debug(f"ftp md5: {ftp_md5}")
#     different = local_md5 != ftp_md5
#     return different


def check_mdt(req, fpath, logger, remoteModDate=None, furl=None,
              head_key='Last-modified'):
    """Check local and remote modified time and return comparison
       You have to pass either the remote last modified date or
       the file url to try to retrieve it
    """
    if not remoteModDate:
        response = req.head(furl)
        remoteModDate = response.headers[head_key]
    remoteModDate = dateutil.parser.parse(remoteModDate)
    localModDate = datetime.fromtimestamp(
                   os.path.getmtime(fpath))
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


# def get_credentials(fname, token=False):
#     """Open file and read username/passowrd or token

#     Requires information to be formatted as
#     1st line: username
#     2nd line: password
#     or if token True
#     1st line: token
#     """

#     f = open(fname, "r")
#     lines = f.readlines()
#     if token:
#         utoken = lines[0].replace("\n","")
#         credentials = (token,)
#     else:
#         uname = lines[0].replace("\n","")
#         passw = lines[1].replace("\n","")
#         credentials = (uname,passw)
#     return credentials

# --------------------------

def parse_input():
    ''' Parse input arguments '''
    parser = argparse.ArgumentParser(description='''Retrieve checksum value for the TRMM HDF 
             files directly from TRMM http server using xml.etree to read the corresponding field. 
             Usage: python gpm-opendap.py -y <year>  ''', formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-y','--year', type=int, help='year to process',
                        required=True)
    parser.add_argument('-u','--user', type=str, help='user account',
                        required=True)
    parser.add_argument('-p','--pwd', type=str, help='account password',
                        default=None, required=False)
    parser.add_argument('-r','--day_range', type=str, help=('Range of days'+
                        ' to download from selected year. ' +
                        'Pass as string "123/125" .'),
                        default="/", required=False)
    parser.add_argument('-d','--debug', action='store_true', required=False,
                        help='Print out debug information, default is False')
    return vars(parser.parse_args())


def download_file(session, url, fname, size, data_log):
    '''Download file using requests '''
    status = 'fine'
    data_log.debug(url)
    r = session.get(url)
    # print(f'Downloading {url}')
    # print(f'r: {r}')
    # print(f'r.content: {r.content}')
    with open(fname, 'wb') as f:
        f.write(r.content)
    del r
    # NB the remote size is actually the HDF5 size not the nc4
    # so local_size should always be bigger than size but we could 
    # still be missing other errors
    local_size = int(os.stat(fname).st_size)
    if local_size < size:
        status = 'error'
    return status 


def download_yr(session, http_url, yr, data_dir, days, data_log):
    '''Download the whole year directory'''
    r = session.get(f"{http_url}/{yr}/contents.html")
    soup = BeautifulSoup(r.content,'html.parser')
    status = {'new': [], 'updated' : [], 'error' : []}
    # find all links with 3 digits indicating day of year folders
    for link in soup.find_all('a',string=re.compile('^\d{3}/')):
        subdir=link.get('href')
        if days != [] and subdir[:3] not in days:
            data_log.debug(f'skipping {subdir[:3]}')
            continue
        r2 = session.get(f"{http_url}/{yr}/{subdir}")
        soup2 = BeautifulSoup(r2.content,'html.parser')
        # the same href file link is repeated in the html,
        # so we need to keep track of what we already checked
        done_list = []
        for sub in soup2.find_all('a', href=re.compile(
                                  '^3B-HHR.*.HDF5.html$')):
            href = sub.get('href')
            sub_next = sub.find_next('td')
            last_mod = sub_next.text.strip()
            size = sub_next.find_next('td').text.strip()
            data_log.debug(f"{href}: {last_mod}, {size}")
            if href in done_list:
                continue
            else:
                done_list.append(href)
                status = process_file(session, data_dir, yr, http_url, 
                    subdir, href, last_mod, int(size), status, data_log)
                if status == "error":
                    print(f'Downloading failed: {http_url}/{subdir}/{href}')
    data_log.info(f"Download for year {yr} is complete")
    return status


def process_file(session, data_dir, yr, http_url, subdir, href, 
                 last_mod, size, status, data_log):
    """Check if file exists and/or needs updating, if new or to update,
       download file
    """
    fname = href.replace('HDF5.html','nc')
    local_name = f"{data_dir}/{yr}/{fname}"
    if not os.path.exists(local_name):
        data_log.debug(f"New file: {local_name}")
        furl = f"{http_url}/{yr}/" + \
               f"{subdir.replace('contents.html','')}" + \
               f"{href.replace('.html','.nc4')}"
        data_log.debug(furl)
        st = download_file(session, furl, local_name, size, data_log)
        if st == 'error':
            status['error'].append(local_name)
        else:
            status['new'].append(local_name)
    else:
        update = check_mdt(session, local_name, data_log,
                           remoteModDate=last_mod)
        if update:
            os.remove(local_name)
            st = download_file(session, furl, local_name, size, data_log)
            if st == 'error':
                status['error'].append(local_name)
            else:
                status['updated'].append(local_name)
    return status


def open_session(usr, pwd):
    '''Open a requests session to manage connection to server '''
    session = requests.session()
    p = session.post("http://urs.earthdata.nasa.gov", {'user':usr,'password':pwd})
    print(f'session.post: {p}')
    cookies=requests.utils.dict_from_cookiejar(session.cookies)
    return session 


def main():
    # read year as external argument and move to data directory
    args = parse_input()
    yr = args['year']
    user = args["user"]
    dr = args["day_range"].split("/")
    # create list of 'days' directories to download
    if dr[0] != "":
        fromd = int(dr[0])
        tod = int(dr[1]) + 1
        days = [str(i).zfill(3) for i in range(fromd, tod)]
    else:
        days =[]
    # get server account password
    try:
        pwd = args["pwd"]
        if pwd is None:
            pwd = os.getenv("GPMPWD")
    except:
        print("Pass a password as input or set the GPMPWD variable")

    # define main directories, user and date 
    today = datetime.today().strftime('%Y-%m-%d')
    sys_user = os.getenv("USER")
    root_dir = os.getenv("AUSREFDIR", ".")
    run_dir = f"{root_dir}"
    # define http_url for GPM-IMERG GESCDISC http server and data_dir for local collection
    http_url = "https://gpm1.gesdisc.eosdis.nasa.gov/opendap/hyrax/GPM_L3/GPM_3IMERGHH.07"
    # we are using a temp dir because we concatenate the files after
    data_dir = f"{root_dir}/data/GPM"
    flog = f"{run_dir}/gpm_update_log.txt"

    # set log
    today = datetime.today().strftime('%Y-%m-%d')
    level = "info"
    if args["debug"]:
        level = "debug"
    data_log = set_log('gpmlog', flog, level)

    # read year as external argument and move to data directory
    # try:
    #     os.chdir(f"{data_dir}/{yr}")
    # except:
    #     os.mkdir(f"{data_dir}/{yr}")
    # Check if the directory exists
    directory = f"{data_dir}/{yr}"
    if not os.path.exists(directory):
        os.mkdir(directory)
        print(f"Directory '{directory}' created.")
    else:
        print(f"Directory '{directory}' already exists.")

    # open a request session and download cookies
    session = open_session(user, pwd)
    status = download_yr(session, http_url, yr, data_dir, days, data_log)#, args["ignored_months"])

    data_log.info(f"Updated on {today} by {sys_user}")
    print_summary(status['updated'], status['new'],
                  status['error'], data_log)
if __name__ == "__main__":
    main()
