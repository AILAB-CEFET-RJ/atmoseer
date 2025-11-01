import os
from datetime import datetime, timedelta
from math import atan2, cos, radians, sin, sqrt

import numpy as np
import pandas as pd
import pytz
from metpy.calc import wind_components
from metpy.units import units

from config import globals


def split_filename(full_filename):
    """
    Splits a full filename into its directory path, base name, and file extension.

    Parameters:
    full_filename (str): The full path of the file, including directory, base name, and extension.

    Returns:
    tuple: A tuple containing:
        - dir_path (str): The directory path.
        - base_name (str): The base name of the file without the extension.
        - file_ext (str): The file extension.

    Examples:
    >>> split_filename('/home/user/docs/report.pdf')
    ('/home/user/docs', 'report', '.pdf')

    >>> split_filename('C:\\Users\\user\\Desktop\\image.png')
    ('C:\\Users\\user\\Desktop', 'image', '.png')

    >>> split_filename('archive.tar.gz')
    ('', 'archive.tar', '.gz')

    >>> split_filename('README')
    ('', 'README', '')
    """
    # Extract the directory path and the filename with extension
    dir_path, filename_with_ext = os.path.split(full_filename)

    # Extract the base name and the extension
    base_name, file_ext = os.path.splitext(filename_with_ext)

    return (dir_path, base_name, file_ext)


def add_missing_indicator_column(df: pd.DataFrame, indicator_col_name: str):
    """
    Add a new column to the given Pandas dataframe with value 1 for rows that have null values
    and 0 for rows that do not have null values.

    Parameters:
    -----------
    df : pandas.DataFrame
        The dataframe to which the new indicator column is to be added.
    indicator_col_name : str
        The name of the new indicator column to be added.

    Returns:
    --------
    pandas.DataFrame
        The modified dataframe with the new indicator column.
    """

    # Create a new column with default value 0
    df[indicator_col_name] = 0

    # Check if any null value is present in each row
    has_null = df.isnull().any(axis=1)

    # Set the value of the missing indicator column to 1 for the rows that have null values
    df.loc[has_null, indicator_col_name] = 1

    return df


def haversine_distance(point1, point2):
    """
    Calculates the Haversine distance between two points on the Earth's surface
    using their latitudes and longitudes.

    Parameters
    ----------
    point1 : tuple
        A tuple of two floats representing the latitude and longitude of the first point.
    point2 : tuple
        A tuple of two floats representing the latitude and longitude of the second point.

    Returns
    -------
    distance : float
        The Haversine distance between the two points, in kilometers.

    References
    ----------
    - Haversine formula: https://en.wikipedia.org/wiki/Haversine_formula
    - Earth's radius: https://en.wikipedia.org/wiki/Earth_radius

    """
    R = 6371  # Earth's radius in kilometers
    lat1, lon1 = point1
    lat2, lon2 = point2
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = (
        sin(dlat / 2) ** 2
        + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    )
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    distance = R * c
    return distance


def get_first_and_last_days_of_year(year: int):
    """
    This  function takes a year as input and returns the first and last days of that year as datetime objects
    """
    assert year <= datetime.now().year

    # Create datetime object for January 1 of the year
    first_day = datetime(year, 1, 1)

    # Calculate the last day of the year as January 1 of the following year minus one day
    last_day = datetime(year + 1, 1, 1) - timedelta(days=1)

    if year == datetime.now().year:
        last_day = min(last_day, datetime.now().today())

    return first_day, last_day


def min_max_normalize(df: pd.DataFrame):
    df = (df - df.min()) / (df.max() - df.min())
    return df


def is_posintstring(s):
    try:
        temp = int(s)
        if temp > 0:
            return True
        else:
            return False
    except ValueError:
        return False


def format_time(input_str):
    """
    This function first converts the input string to an integer using the int function.
    It then extracts the hours and minutes from the input integer using integer division and modulus operations.
    Finally, it formats the output string using f-strings to ensure that both hours and minutes are represented with two digits.
    Usage examples:
        print(format_time("100"))   # Output: "01:00"
        print(format_time("1200"))  # Output: "12:00"
        print(format_time("2300"))  # Output: "23:00"
    """
    # Convert input string to integer
    input_int = int(input_str)

    # Extract hours and minutes from the input integer
    hours = input_int // 100
    minutes = input_int % 100

    # Format the output string
    output_str = f"{hours:02}:{minutes:02}"

    return output_str


def utc_to_local_DEPRECATED(utc_string, local_tz, format_string):
    """
    This function first converts the UTC string to a datetime object with timezone information
    using strptime() and replace(). Then it converts the datetime object to the local timezone
    using astimezone(). Finally, it formats the resulting datetime as a string in the same
    format as the input string.

    Here's an example usage of the function:

        utc_string = '1972-09-13 12:00:00'
        local_tz = 'America/Sao_Paulo'
        format_string = '%Y-%m-%d %H:%M'
        local_string = utc_to_local(utc_string, local_tz, format_string)
        print(local_string)

    """
    # convert utc string to datetime object
    utc_dt = datetime.strptime(utc_string, format_string).replace(tzinfo=pytz.UTC)

    # convert to local timezone
    local_tz = pytz.timezone(local_tz)
    local_dt = utc_dt.astimezone(local_tz)

    # format as string and return
    return local_dt.strftime(format_string)


def transform_wind(wind_speed, wind_direction, comp_idx):
    """
    This function calculates either the U or V wind vector component from the speed and direction.
    comp_idx = 0 --> computes the U component
    comp_idx = 1 --> computes the V component
    (see https://unidata.github.io/MetPy/latest/api/generated/metpy.calc.wind_components.html)
    """
    assert comp_idx == 0 or comp_idx == 1
    return wind_components(wind_speed * units("m/s"), wind_direction * units.deg)[
        comp_idx
    ].magnitude


def add_datetime_index(station_id, df):
    timestamp = None
    if station_id in globals.INMET_WEATHER_STATION_IDS:
        df.HR_MEDICAO = df.HR_MEDICAO.apply(format_time)  # e.g., 1800 --> 18:00
        timestamp = pd.to_datetime(df.DT_MEDICAO + " " + df.HR_MEDICAO)
    elif station_id in globals.ALERTARIO_WEATHER_STATION_IDS:
        timestamp = pd.to_datetime(df["datetime"])
        timestamp = timestamp.dt.tz_convert("UTC")
    assert timestamp is not None
    df = df.set_index(pd.DatetimeIndex(timestamp))
    return df


def add_wind_related_features(station_id, df):
    df["wind_direction_u"] = df.apply(
        lambda x: transform_wind(x.wind_speed, x.wind_dir, 0), axis=1
    )
    df["wind_direction_v"] = df.apply(
        lambda x: transform_wind(x.wind_speed, x.wind_dir, 1), axis=1
    )
    return df


def add_hour_related_features(df):
    """
    Transforms a DataFrame's datetime index into two new columns representing the hour in sin and cosine form.
    (see https://datascience.stackexchange.com/questions/5990/what-is-a-good-way-to-transform-cyclic-ordinal-attributes)

    Args:
    - df: A pandas DataFrame with a datetime index.

    Returns:
    - The input pandas DataFrame with two new columns named 'hour_sin' and 'hour_cos' representing the hour in sin and cosine form.
    """
    dt = df.index
    hourfloat = dt.hour + dt.minute / 60.0
    df["hour_sin"] = np.sin(2.0 * np.pi * hourfloat / 24.0)
    df["hour_cos"] = np.cos(2.0 * np.pi * hourfloat / 24.0)
    return df


def get_filename_and_extension(filename):
    """
    Given a filename, returns a tuple with the base filename and extension.
    """
    basename = os.path.basename(filename)
    filename_parts = os.path.splitext(basename)
    return filename_parts


def find_contiguous_observation_blocks(df: pd.DataFrame):
    """
    Given a list of timestamps, finds contiguous blocks of timestamps that are exactly one hour apart of each other.

    Args:
    - timestamp_range: A list-like object of pandas Timestamps.

    Yields:
    - A tuple representing a contiguous block of timestamps: (start, end).

    Usage example:
    >>> period_under_study = df['2007-05-18':'2007-05-31']
    >>> contiguous_observations = list(find_contiguous_observation_blocks(period_under_study))
    >>> print(len(contiguous_observations))
    >>> print(contiguous_observations)
    5
    [(Timestamp('2007-05-18 18:00:00'), Timestamp('2007-05-18 19:00:00')),
     (Timestamp('2007-05-19 11:00:00'), Timestamp('2007-05-19 13:00:00')),
     (Timestamp('2007-05-20 12:00:00'), Timestamp('2007-05-21 00:00:00')),
     (Timestamp('2007-05-21 02:00:00'), Timestamp('2007-05-21 08:00:00')),
     (Timestamp('2007-05-21 10:00:00'), Timestamp('2007-05-31 23:00:00'))]

    In this example, `timestamp_range` is a list of pandas Timestamps extracted from a DataFrame's 'Datetime' index.
    The function finds contiguous blocks of timestamps that are exactly one hour apart, and yields tuples representing these blocks.
    The `yield` statement produces a generator object, which can be converted to a list using the `list()` function.

    Returns:
    - None
    """
    timestamp_range = df.index
    assert len(timestamp_range) > 1
    first = last = timestamp_range[0]
    for n in timestamp_range[1:]:
        previous = n - timedelta(hours=1, minutes=0)
        if previous == last:  # Part of the current block, bump the end
            last = n
        else:  # Not part of the current block; yield current block and start a new one
            yield first, last
            first = last = n
    yield first, last  # Yield the last block


def get_relevant_variables(station_id):
    if station_id in globals.INMET_WEATHER_STATION_IDS:
        return [
            "temperature",
            "barometric_pressure",
            "relative_humidity",
            "wind_direction_u",
            "wind_direction_v",
            "hour_sin",
            "hour_cos",
        ], "precipitation"
    elif station_id in globals.ALERTARIO_WEATHER_STATION_IDS:
        return [
            "temperature",
            "barometric_pressure",
            "relative_humidity",
            "wind_direction_u",
            "wind_direction_v",
            "hour_sin",
            "hour_cos",
        ], "precipitation"
    elif station_id in globals.ALERTARIO_GAUGE_STATION_IDS:
        return [
            "temperature",
            "barometric_pressure",
            "relative_humidity",
            "wind_direction_u",
            "wind_direction_v",
            "hour_sin",
            "hour_cos",
        ], "precipitation"
    return None


def convert_to_celsius(temperature_kelvin):
    """Converts a temperature from Kelvin to Celsius.

    Args:
      temperature_kelvin: The temperature in Kelvin.

    Returns:
      The temperature in Celsius.
    """

    return temperature_kelvin - 273.15


def rename_dataframe_column_names(df: pd.DataFrame, column_name_mapping: dict):
    """
    Renames the column names in the DataFrame according to the specified dictionary.

    Args:
      df: The DataFrame to rename.
      column_name_mapping: The dictionary that maps old column names to new column names.

    Returns:
      The renamed DataFrame.
    """

    new_column_names = []
    for old_column_name, new_column_name in column_name_mapping.items():
        if old_column_name in df.columns:
            new_column_names.append(new_column_name)
        else:
            print(f"The column name {old_column_name} does not exist in the DataFrame.")

    df.columns = new_column_names
    return df


def get_dataframe_with_selected_columns(df, column_names):
    """
    Returns a DataFrame containing only the columns whose names are passed in the list.

    Args:
      df: The DataFrame to select columns from.
      column_names: The list of column names to select.

    Returns:
      A DataFrame containing only the selected columns.
    """

    selected_columns = []
    for column_name in column_names:
        if column_name in df.columns:
            selected_columns.append(column_name)
        else:
            print(f"The column name {column_name} does not exist in the DataFrame.")

    return df[selected_columns]


def split_dataframe_by_date(df, split_date):
    """
    Split a DataFrame into two DataFrames: one with examples before the given date
    and the other with examples after the given date.

    Args:
    - df: pandas DataFrame with a datetime index.
    - split_date: datetime object specifying the date for splitting the DataFrame.

    Returns:
    - df_before: DataFrame with examples before the split_date.
    - df_after: DataFrame with examples after the split_date.
    """

    # Ensure the DataFrame is sorted by the datetime index
    df = df.sort_index()

    # Split the DataFrame based on the provided split_date
    # print(split_date)
    # print(df.index[0])
    # print(type(split_date))
    # print(type(df.index[0]))
    # print(type(df.index))
    # print(type(df.index.to_pydatetime()))

    df_before = df[df.index < split_date]
    df_after = df[df.index >= split_date]

    return df_before, df_after
