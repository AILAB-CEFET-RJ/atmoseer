import pandas as pd
import numpy as np
import os
from typing import Tuple

def add_datetime_index(station_id: str, df: pd.DataFrame) -> pd.DataFrame:
    """Sets the 'datetime' column as the DataFrame's index."""
    if 'datetime' not in df.columns:
        raise ValueError(f"Column 'datetime' not found for station {station_id}.")
    
    # Convert to datetime and handle errors
    df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
    
    # Remove rows where date couldn't be converted
    df = df.dropna(subset=['datetime'])
    
    if df.empty:
        return df

    # Set index and sort
    df = df.set_index('datetime').sort_index()
    
    # Remove duplicates, keeping the first occurrence
    df = df[~df.index.duplicated(keep='first')]
    
    return df

def get_dataframe_with_selected_columns(df: pd.DataFrame, column_names: list) -> pd.DataFrame:
    """Returns a DataFrame containing only the specified columns."""
    # Filter only columns that actually exist in the DataFrame
    cols_to_keep = [col for col in column_names if col in df.columns]
    return df[cols_to_keep]

def rename_dataframe_column_names(df: pd.DataFrame, column_mapping: dict) -> pd.DataFrame:
    """Renames the DataFrame columns based on the mapping."""
    # Filter the mapping to include only existing columns
    valid_mapping = {k: v for k, v in column_mapping.items() if k in df.columns}
    return df.rename(columns=valid_mapping)

def get_relevant_variables(station_id: str) -> Tuple[list, str]:
    """Fallback function (should not be used if the JSON is complete)."""
    print(f"Warning: get_relevant_variables (fallback) called for {station_id}")
    return [], ""

def add_wind_related_features(station_id: str, df: pd.DataFrame) -> pd.DataFrame:
    """Adds U and V wind components."""
    if 'wind_speed' in df.columns and 'wind_dir' in df.columns:
        # Convert wind direction (degrees) to radians
        wind_dir_rad = df['wind_dir'] * np.pi / 180.0
        
        # Calculate U (East-West) and V (North-South) components
        df['wind_direction_u'] = -df['wind_speed'] * np.sin(wind_dir_rad)
        df['wind_direction_v'] = -df['wind_speed'] * np.cos(wind_dir_rad)
    else:
        print("Warning: 'wind_speed' or 'wind_dir' columns not found. Skipping wind features.")
    return df

def add_hour_related_features(df: pd.DataFrame) -> pd.DataFrame:
    """Adds sin/cos components of the hour of the day."""
    if not isinstance(df.index, pd.DatetimeIndex):
        print("Warning: Index is not a DatetimeIndex. Skipping hour features.")
        return df
        
    hour_of_day = df.index.hour
    # Normalize the hour for the 24h cycle
    df['hour_sin'] = np.sin(2 * np.pi * hour_of_day / 24.0)
    df['hour_cos'] = np.cos(2 * np.pi * hour_of_day / 24.0)
    return df

def get_filename_and_extension(filepath: str) -> Tuple[str, str]:
    """Separates the filename from its extension."""
    # Get the base name (e.g., '22.csv')
    basename = os.path.basename(filepath) 
    # Separate name and extension (e.g., ('22', '.csv'))
    filename, extension = os.path.splitext(basename)
    return filename, extension

def min_max_normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Normalizes the DataFrame using the Min-Max formula (0 to 1)."""
    df_min = df.min()
    df_max = df.max()
    range = df_max - df_min
    
    # Avoid division by zero if a column is constant
    range[range == 0] = 1.0
    
    return (df - df_min) / range