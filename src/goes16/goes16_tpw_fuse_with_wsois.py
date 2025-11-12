import pandas as pd
from config.globals import INMET_WEATHER_STATION_IDS
import os

def hourly_average_with_nan_handling(df: pd.DataFrame):
    """
    Resamples a DataFrame containing time series data to an hourly frequency and computes the mean of 
    the 'tpw_value' column.
    
    Parameters:
    df (pd.DataFrame): A pandas DataFrame with a DateTime index and a column named 'tpw_value' containing 
                    the time series data.
    
    Returns:
    pd.DataFrame: A DataFrame with the hourly resampled mean values of the 'tpw_value' column. 
                    The index of the returned DataFrame is shifted by one hour.
    
    Notes:
    - The function resamples the input DataFrame to an hourly frequency and calculates the mean of the 'tpw_value' column for each hour.
    - NaN values are ignored in the mean calculation.
    - The index of the resulting DataFrame is shifted by one hour using `pd.DateOffset(hours=1)` to correctly represent the aggregated value in the previous hour.
    - If all the values in a hour are NaN, the the corresponding entry in the resulting dataframe will also be NaN.
    
    Example:
    df = pd.DataFrame({'tpw_value': [2, 2, 2, np.NaN, 2, 2, 3, 3, 3, 3, 3, 3, np.NaN, 4, 4, 4, np.NaN]}, 
                        index=pd.date_range('2023-01-01', periods=17, freq='10T'))
    print(df)
    print(30*'~')
    print(hourly_average_with_nan_handling(df))

                            tpw_value
        2023-01-01 00:00:00        2.0
        2023-01-01 00:10:00        2.0
        2023-01-01 00:20:00        2.0
        2023-01-01 00:30:00        NaN
        2023-01-01 00:40:00        2.0
        2023-01-01 00:50:00        2.0
        2023-01-01 01:00:00        3.0
        2023-01-01 01:10:00        3.0
        2023-01-01 01:20:00        3.0
        2023-01-01 01:30:00        3.0
        2023-01-01 01:40:00        3.0
        2023-01-01 01:50:00        3.0
        2023-01-01 02:00:00        NaN
        2023-01-01 02:10:00        4.0
        2023-01-01 02:20:00        4.0
        2023-01-01 02:30:00        4.0
        2023-01-01 02:40:00        NaN
        ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
                            tpw_value
        2023-01-01 01:00:00        2.0
        2023-01-01 02:00:00        3.0
        2023-01-01 03:00:00        4.0
    """
    # Resample the DataFrame to hourly frequency and compute the mean
    df_resampled = df['tpw_value'].resample('H').mean()
    
    return pd.DataFrame({'tpw_value': df_resampled.values},
                        index=df_resampled.index + pd.DateOffset(hours=1))


if __name__ == "__main__":
    # Set the folder containing the TPW parquet files
    folder_path = './data/goes16/tpw'

    # Initialize an empty list to store DataFrames
    dataframes = []

    # Create list with all Parquet files in the folder
    parquet_files = [file for file in os.listdir(folder_path) if file.endswith('.parquet')]

    # Loop through the Parquet files, read them, and append to the list of Dataframes
    for file in parquet_files:
        file_path = os.path.join(folder_path, file)
        df = pd.read_parquet(file_path)
        print(f'Merging dataframe {file_path} of shape {df.shape}')
        dataframes.append(df)

    # Concatenate all DataFrames into a single DataFrame, so that
    # merged_df contains the combined data from all Parquet files
    merged_df = pd.concat(dataframes)

    # print(f'merged_df.columns (1): {merged_df.columns}')

    # # Reset the index to 'timestamp' and drop the old index
    # merged_df.set_index('timestamp', inplace=True)
    # merged_df.index = pd.to_datetime(merged_df.index[0])

    # print(f'merged_df.columns (2): {merged_df.columns}')

    # If you want to sort the DataFrame by the 'timestamp' column
    # merged_df.sort_index(inplace=True)


    # Print the first few rows to verify
    print('MERGED DATAFRAME:')
    print(merged_df.head())
    print(merged_df.shape)

    for wsoi_id in INMET_WEATHER_STATION_IDS:
        print(f'Processing data for station {wsoi_id}')

        # Create a boolean mask to filter rows with 'station_id' equal to 'wsoi_id'
        mask = merged_df['station_id'] == wsoi_id

        # Use the mask to select rows from the DataFrame
        df_wsoi = merged_df[mask]
        print(df_wsoi)

        print('columns (1):', df_wsoi.columns)
        df_wsoi.reset_index(inplace=True)
        print('columns (2):', df_wsoi.columns)

        # Now, the `df_wsoi` DataFrame contains rows where 'station_id' matches 'wsoi_id'
        print(f'df_wsoi.shape: {df_wsoi.shape}')
        print('Number of NaN values:', df_wsoi['tpw_value'].isna().sum())

        print(f'df_wsoi.index (1): {df_wsoi.index}')

        print(f'---(1):')
        print(df_wsoi.head())

        df_wsoi = df_wsoi[['timestamp', 'tpw_value']]

        df_wsoi.index = pd.to_datetime(df_wsoi.timestamp)
        df_wsoi = df_wsoi.drop(columns=['timestamp'])

        df_wsoi = df_wsoi.sort_index()

        print(f'---(2):')
        print(df_wsoi.head(80))

        df_wsoi = hourly_average_with_nan_handling(df_wsoi)

        print(f'df_wsoi.index (2): {df_wsoi.index}')

        print(f'---(3):')
        print(df_wsoi.head(30))

        print(df_wsoi['tpw_value'].isna().sum())
        df_wsoi.to_parquet(f'./data/goes16/wsoi/{wsoi_id}.parquet')
        print('~~~')
