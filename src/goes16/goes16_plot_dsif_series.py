from datetime import datetime  # Basic Dates and time types
import os
import sys
import argparse
import xarray as xr  # type: ignore
import pandas as pd  # type: ignore
import matplotlib.pyplot as plt  # type: ignore


def goes16_data_retrieve(begin_date, end_date, lat_point, lon_point, agg_function):

    extent = [-74.0, -34.1, -34.8, 5.5]  # Min lon, Min lat, Max lon, Max lat

    min_lon = extent[0]
    min_lat = extent[1]
    max_lon = extent[2]
    max_lat = extent[3]

    dir_list = os.listdir(path_goes16)
    goes16_data = pd.DataFrame(columns=['time', 'goes16_cape', 'goes16_ki', 'goes16_si', 'goes16_li', 'goes16_tt'])

    for file in dir_list:
        current_date = datetime.strptime(file[0:12], '%Y%m%d%H%M')
        if begin_date <= current_date <= end_date:
            data = pd.read_pickle(path_goes16 + file)
            cape = pd.DataFrame(data["CAPE"])
            ki = pd.DataFrame(data["KI"])
            si = pd.DataFrame(data["SI"])
            li = pd.DataFrame(data["LI"])
            tt = pd.DataFrame(data["TT"])

            num_points_x, num_points_y = cape.shape

            axis_y = (min_lat - max_lat) / num_points_y
            axis_x = (max_lon - min_lon) / num_points_x

            y = int((float(lat_point) - max_lat) / axis_y)
            x = int((float(lon_point) - min_lon) / axis_x)

            goes16_data.loc[len(goes16_data.index)] = [current_date,  cape.iat[x, y], ki.iat[x, y], si.iat[x, y],
                                                       li.iat[x, y], tt.iat[x, y]]

    goes16_data = goes16_data.sort_values('time')

    print(f"Aggregating GOES16 Data, using {agg_function}...")
    if (agg_function == 'max'):
        goes16_data_agg = goes16_data.resample('2h', on='time').max()
    elif (agg_function == 'min'):
        goes16_data_agg = goes16_data.resample('2h', on='time').min()
    elif (agg_function == 'mean'):
        goes16_data_agg = goes16_data.resample('2h', on='time').mean()

    goes16_data_agg = goes16_data_agg.reset_index()
    return goes16_data_agg


def eras5_data_retrieve(lat, lon):
    ds = xr.open_dataset(era5_path)
    ds_combine = ds.sel(expver=1).combine_first(ds.sel(expver=5))
    ds_combine.load()
    ds = ds_combine

    # Select ERA5 data variable measured in a location nearest to the SBGL radiosonde station
    ds_era5_data_nearest_to_SBGL = ds.sel(longitude=lon, latitude=lat, method="nearest")

    # Put the selected variables in a Pandas DataFrame
    df_era5_data_nearest_to_SBGL = pd.DataFrame(
            {
                "time": ds_era5_data_nearest_to_SBGL.time.values,
                "ERA5_cape": ds_era5_data_nearest_to_SBGL.cape,
                "ERA5_ki": ds_era5_data_nearest_to_SBGL.kx,
                "ERA5_tt": ds_era5_data_nearest_to_SBGL.totalx
            }
        )

    return df_era5_data_nearest_to_SBGL


def sbgl_data_retrieve():
    sbgl_data = pd.read_parquet(path_sbgl, engine='pyarrow')
    sbgl_data.rename(columns={'cape': 'sbgl_cape', 'cin': 'sbgl_cin', 'k': 'sbgl_ki', 'lift': 'sbgl_li', 
                              'total_totals': 'sbgl_tt', 'showalter': 'sbgl_si'}, inplace=True)

    return sbgl_data


def plot_dsif_index_values(df, start_date, end_date, index):
    # Filter the DataFrame to include only the data within the specified date range
    filtered_df = df[start_date:end_date]
    filtered_df = filtered_df.dropna(subset=['goes16_' + index])

    # Plot the data
    plt.figure(figsize=(10, 5))
    plt.plot(filtered_df.index, filtered_df['goes16_' + index].astype(dtype='Float64'), label='GOES16')
    plt.plot(filtered_df.index, filtered_df['sbgl_' + index].astype(dtype='Float64'), label='SBGL')
    if index in ('cape', 'ki', 'tt'):
        plt.plot(filtered_df.index, filtered_df['ERA5_' + index].astype(dtype='Float64'), label='ERA5')
    # Adding title and labels
    plt.title(f'{index} values from {start_date} to {end_date}')
    plt.xlabel('Date')
    plt.ylabel(index)

    # Adding a legend
    plt.legend()

    plt.savefig(root_path + index + "_" + datetime.strptime(str(start_date), '%Y-%m-%d %H:%M:%S').strftime('%Y%m%d%H%M')
                + "_" + str(datetime.strptime(str(end_date), '%Y-%m-%d %H:%M:%S').strftime('%Y%m%d%H%M')) + ".png")

    print("plot saved at " + root_path + index + "_" + datetime.strptime(str(start_date), '%Y-%m-%d %H:%M:%S').strftime(
        '%Y%m%d%H%M') + "_" + str(datetime.strptime(str(end_date), '%Y-%m-%d %H:%M:%S').strftime('%Y%m%d%H%M')) + ".png"
        )
    # Display the plot
    plt.show()


def join_dataframes_on_datetime(df1, df2, datetime_col='datetime'):
    """
    Joins two Pandas DataFrames on a datetime column.

    Parameters:
    df1 (pd.DataFrame): The first dataframe.
    df2 (pd.DataFrame): The second dataframe.
    datetime_col (str): The name of the datetime column to join on. Default is 'datetime'.

    Returns:
    pd.DataFrame: The joined dataframe.
    """
    if datetime_col not in df1.columns or datetime_col not in df2.columns:
        raise ValueError(f"Both dataframes must contain the column '{datetime_col}'")

    if not pd.api.types.is_datetime64_any_dtype(df1[datetime_col]) or not pd.api.types.is_datetime64_any_dtype(
                                                                                                df2[datetime_col]):
        raise TypeError(f"The column '{datetime_col}' must be of type datetime64 in both dataframes")

    joined_df = pd.merge(df1, df2, on=datetime_col)

    return joined_df


def main(argv):
    parser = argparse.ArgumentParser(prog=argv[0],
                                     description="""This script provides a simple interface to build and plot
                                     a time series for indexes  """)
    parser.add_argument("-b", "--begin_date", required=True, help="Begin date", metavar='')
    parser.add_argument("-e", "--end_date", required=False, help="End date", metavar='')
    parser.add_argument('-lat', "--point_latitude", help='Latitude of the current point', required=False)
    parser.add_argument('-lon', "--point_longitude", help='Longitude of the current point', required=False)
    parser.add_argument('-i', "--dsif_index", help='DSIF index to be plotted', required=False)
    args = parser.parse_args(argv[1:])

    begin_date_string = args.begin_date
    end_date_string = args.end_date
    lat_point = args.point_latitude
    lon_point = args.point_longitude
    dsif_index = args.dsif_index

    print('-' * 100)
    times_of_day = ['0000', '1200']

    begin_date = datetime.strptime(begin_date_string + times_of_day[0], '%Y%m%d%H%M')
    end_date = datetime.strptime(end_date_string + times_of_day[1], '%Y%m%d%H%M')

    print("Retriveving GOES16 Data...")
    goes16_data = goes16_data_retrieve(begin_date, end_date, lat_point, lon_point, agg_function='mean')
    print("Retriveving ERA5 Data...")
    era5_data = eras5_data_retrieve(lat_point, lon_point)
    print("Retriveving SBGL Data...")
    sbgl_data = sbgl_data_retrieve()

    print("Joining datasets...")
    joined_df = join_dataframes_on_datetime(goes16_data, sbgl_data, datetime_col='time')
    joined_df = join_dataframes_on_datetime(joined_df, era5_data, datetime_col='time')
    joined_df.set_index('time', inplace=True)
    plot_dsif_index_values(joined_df, begin_date, end_date, dsif_index)

    print('-' * 100)


if __name__ == "__main__":
    root_path = "./data/"
    path_goes16 = "./data/goes16/dsif/"
    path_sbgl = "./data/sbgl/SBGL_indices_1997_2023.parquet.gzip"
    era5_path = "./data/ERA5/RJ_1997_2024.nc"
    main(sys.argv)

# python plot_dsif_series.py -b 20200101 -e 20200131 -lat -22.8089 -lon -43.2436 -i cape
