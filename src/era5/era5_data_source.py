from base_data_source import BaseDataSource
import logging
import pandas as pd
import xarray as xr

class Era5ReanalisysDataSource(BaseDataSource):
    def get_data(self, station_id, initial_datetime, final_datetime):
        df_stations = pd.read_csv("./data/ws/WeatherStations.csv")
        row = df_stations[df_stations["STATION_ID"] == station_id].iloc[0]
        station_latitude = row["VL_LATITUDE"]
        station_longitude = row["VL_LONGITUDE"]

        logging.info(f"Weather station {station_id} is located at lat/long = {station_latitude}/{station_longitude}")

        logging.info(f"Selecting NWP data between {initial_datetime} and {final_datetime}.")
        
        ds = xr.open_dataset(globals.NWP_DATA_DIR + "ERA5.nc")
        logging.info(f"Size.0: {ds.sizes['time']}")

        # Get the minimum and maximum values of the 'time' coordinate
        # time_min = ds.coords['time'].min().item()
        # time_max = ds.coords['time'].max().item()
        # logging.info(f"Range of timestamps in the original NWP data: [{ds.time.min()}, {ds.time.max()}]")
        # logging.info(f"Range of timestamps in the original NWP data: [{time_min}, {time_max}]")
        time_min = ds.time.min().values
        time_max = ds.time.max().values
        logging.info(f"Range of timestamps in the original NWP data: [{time_min}, {time_max}]")

        # If we want to properly merge the two data sources, then we have to consider 
        # only the range of periods in which these data sources intersect.
        time_min = max(time_min, initial_datetime)
        time_max = min(time_max, final_datetime)
        logging.info(f"Range of timestamps to be selected: [{time_min}, {time_max}]")

        ds = ds.sel(time=slice(time_min, time_max))
        logging.info(f"Size.1: {ds.sizes['time']}")

        era5_data_at_200hPa = ds.sel(level=200, longitude=station_longitude, latitude=station_latitude, method="nearest")
        logging.info(f"Size.2: {era5_data_at_200hPa.sizes['time']}")

        era5_data_at_700hPa = ds.sel(level=700, longitude=station_longitude, latitude=station_latitude, method="nearest")
        logging.info(f"Size.3: {era5_data_at_700hPa.sizes['time']}")

        era5_data_at_1000hPa = ds.sel(level=1000, longitude=station_longitude, latitude=station_latitude, method="nearest")
        logging.info(f"Size.4: {era5_data_at_1000hPa.sizes['time']}")

        logging.info(">>><<<")
        logging.info(type(era5_data_at_1000hPa.time))
        logging.info("-1-")
        logging.info(era5_data_at_200hPa.time.values)
        logging.info("-2-")
        logging.info(era5_data_at_200hPa.z.values)
        logging.info("-3-")
        logging.info(era5_data_at_700hPa.z.values.shape)
        logging.info("-4-")
        logging.info(era5_data_at_700hPa.time.values)
        logging.info("-5-")
        logging.info(era5_data_at_700hPa.z.values)
        logging.info("-6-")
        logging.info(era5_data_at_700hPa.z.values.shape)
        logging.info(">>><<<")

        df_NWP_data_for_station = pd.DataFrame(
            {
                "time": era5_data_at_1000hPa.time.values,
                
                "Geopotential_200": era5_data_at_200hPa.z,
                "Humidity_200": era5_data_at_200hPa.r,
                "Temperature_200": era5_data_at_200hPa.t,
                "WindU_200": era5_data_at_200hPa.u,
                "WindV_200": era5_data_at_200hPa.v,

                "Geopotential_700": era5_data_at_700hPa.z,
                "Humidity_700": era5_data_at_700hPa.r,
                "Temperature_700": era5_data_at_700hPa.t,
                "WindU_700": era5_data_at_700hPa.u,
                "WindV_700": era5_data_at_700hPa.v,

                "Geopotential_1000": era5_data_at_1000hPa.z,
                "Humidity_1000": era5_data_at_1000hPa.r,
                "Temperature_1000": era5_data_at_1000hPa.t,
                "WindU_1000": era5_data_at_1000hPa.u,
                "WindV_1000": era5_data_at_1000hPa.v
            }
        )

        # Drop rows with at least one NaN
        logging.info(f"Shape before dropping NaN values is {df_NWP_data_for_station.shape}")
        df_NWP_data_for_station = df_NWP_data_for_station.dropna(how='any')
        logging.info(f"Shape before dropping NaN values is {df_NWP_data_for_station.shape}")

        logging.info("Success!")

        #
        # Add index to dataframe using the timestamps.
        format_string = '%Y-%m-%d %H:%M:%S'
        df_NWP_data_for_station['Datetime'] = pd.to_datetime(df_NWP_data_for_station['time'], format=format_string)
        df_NWP_data_for_station = df_NWP_data_for_station.set_index(pd.DatetimeIndex(df_NWP_data_for_station['Datetime']))
        df_NWP_data_for_station = df_NWP_data_for_station.drop(['time', 'Datetime'], axis = 1)
        logging.info(f"Range of timestamps in the selected slice of NWP data: [{min(df_NWP_data_for_station.index)}, {max(df_NWP_data_for_station.index)}]")

        logging.info(df_NWP_data_for_station)

        assert (not df_NWP_data_for_station.isnull().values.any().any())

        return df_NWP_data_for_station
