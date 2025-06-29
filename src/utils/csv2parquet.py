import argparse
import os
import pyarrow.parquet as pq
import pandas as pd

def convert_csv_to_parquet(input_folder, output_folder):
  """
  Converts all CSV files in the input folder to Parquet files in the output folder.

  Args:
    input_folder: The folder containing the CSV files.
    output_folder: The folder where the Parquet files will be saved.
  """

  for file_name in os.listdir(input_folder):
    if file_name.endswith(".csv"):
      csv_file_path = os.path.join(input_folder, file_name)
      parquet_file_path = os.path.join(output_folder, file_name.replace(".csv", ".parquet"))
      df = pd.read_csv(csv_file_path)
      df.to_parquet(parquet_file_path)


if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("input_folder", help="The folder containing the CSV files.")
  parser.add_argument("output_folder", help="The folder where the Parquet files will be saved.")
  args = parser.parse_args()

  convert_csv_to_parquet(args.input_folder, args.output_folder)
