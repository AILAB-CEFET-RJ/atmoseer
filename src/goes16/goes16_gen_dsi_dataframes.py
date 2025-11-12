import pandas as pd
import os
from netCDF4 import Dataset
from collections import OrderedDict
import time
import sys
import logging
import argparse
from utils.util import split_filename

def main(argv):
    # Set the folder containing the DSI files
    input_folder_path = './data/goes16/DSI'

    dsi_variable_names = ['CAPE', 'LI', 'TT', 'SI', 'KI']

    for variable_name in dsi_variable_names:
        df = None

        # Create a list to store all the variable's timestamped files in the folder and subfolders
        filenames = []
        for root, dirs, files in os.walk(input_folder_path):
            for file in files:
                if file.endswith(f'_{variable_name}.nc'):
                # if file.startswith('2019') and file.endswith(f'_{variable_name}.nc'):
                    filenames.append(os.path.join(root, file))

        logging.info(f'Total number of files: {len(filenames)}')

        # Loop through the selected timestamped files
        counter = 0
        for filename in filenames:
            if (counter > 0) and (counter % 5000 == 0):
                logging.info(f'Number of processed files: {counter}')
            counter += 1

            file = Dataset(f'{filename}')

            # The timestamp is a substring of the filename!
            dir_path, base_name, file_ext = split_filename(filename)
            yyyymmddhhmn = base_name.partition('_')[0]

            # Get the pixel values
            data = file.variables['Band1'][:]

            if df is None:
                feature_names = []
                for y in range(data.shape[0]):
                    for x in range(data.shape[1]):
                        feature_names += [f'{variable_name}{y}{x}']
                df = pd.DataFrame(columns=['timestamp', *feature_names])

            feature_values = []
            for y in range(data.shape[0]):
                for x in range(data.shape[1]):
                    feature_values += [data[y,x]]
            
            # See https://stackoverflow.com/questions/70837397/good-alternative-to-pandas-append-method-now-that-it-is-being-deprecated
            df.loc[len(df), df.columns] = [yyyymmddhhmn] + feature_values

        logging.info(f"Creating index in dataframe...")

        # Set the index to 'timestamp'
        timestamp = pd.to_datetime(df.timestamp)
        df = df.set_index(pd.DatetimeIndex(timestamp))

        # Sort the DataFrame by the 'timestamp' column
        df.sort_index(inplace=True)

        logging.info(f"Done!")

        df_filename = f'{variable_name}.parquet'
        df.to_parquet(df_filename)
        logging.info(f'A Pandas dataframe with shape {df.shape} was created and saved in the file {df_filename}.')

if __name__ == "__main__":
    fmt = "[%(levelname)s] %(funcName)s():%(lineno)i: %(message)s"
    logging.basicConfig(level=logging.DEBUG, format = fmt)

    start_time = time.time()  # Record the start time

    main(sys.argv)
    
    end_time = time.time()  # Record the end time
    duration = (end_time - start_time) / 60  # Calculate duration in minutes
    
    logging.info(f"Script duration: {duration:.2f} minutes")    
