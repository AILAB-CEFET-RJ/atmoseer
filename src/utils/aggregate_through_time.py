import pandas as pd
import os
import time
import sys
import logging
import argparse
from netCDF4 import Dataset
from collections import OrderedDict

def aggregate_to_hourly_resolution(df: pd.DataFrame):
    assert (not df.isnull().values.any().any())

    # Resample the DataFrame to hourly frequency and compute the mean
    hourly_df = df.resample('H').mean()

    hourly_df.set_index(hourly_df.index + pd.DateOffset(hours=1), inplace=True)

    hourly_df = hourly_df.dropna()
    
    return hourly_df

def main(argv):
    parser = argparse.ArgumentParser(description="Aggregate observations of a Pandas dataframe with a datetime as index to hourly temporal resolution. Results are saved as another Pandas dataframe.")
    parser.add_argument('--input_file', type=str, required=True, help='Path to the input Parquet file')
    parser.add_argument('--output_file', type=str, required=True, help='Path to the output Parquet file')

    args = parser.parse_args()

    input_file = args.input_file
    output_file = args.output_file

    df = pd.read_parquet(input_file)
    logging.info(f'The input dataframe has shape {df.shape}.')

    agg_df = aggregate_to_hourly_resolution(df)
    agg_df.to_parquet(output_file)
    logging.info(f'The output dataframe with shape {agg_df.shape} was created.')

# python src/aggregate_through_time.py --input_file ./data/goes16/DSI/DSI_CAPE.parquet --output_file ./data/goes16/DSI/DSI_CAPE_1H.parquet
if __name__ == "__main__":
    fmt = "[%(levelname)s] %(funcName)s():%(lineno)i: %(message)s"
    logging.basicConfig(level=logging.DEBUG, format = fmt)

    start_time = time.time()  # Record the start time

    main(sys.argv)
    
    end_time = time.time()  # Record the end time
    duration = (end_time - start_time) / 60  # Calculate duration in minutes
    
    logging.info(f"Script duration: {duration:.2f} minutes")    
