import pandas as pd
import xarray as xr

from utils import util


def fuse_rain_gauge_and_era5(
    df_station: pd.DataFrame, station_latitude: float, station_longitude: float, ds_era5
):
    df_station["datetime"] = df_station["datetime"].dt.tz_convert("UTC")
    df_station["datetime"] = df_station["datetime"].dt.tz_localize(None)
    df_station = df_station.set_index(pd.DatetimeIndex(df_station["datetime"]))
    df_station = df_station.drop(["datetime"], axis=1)
    assert not df_station.isnull().values.any().any()

    time_min = min(df_station.index)
    time_max = max(df_station.index)
    print(f"Range of timestamps: [{time_min}, {time_max}]")
    print(f"Number of observations (gauge station): {df_station.shape[0]}")

    # Select ERA5 data near the station
    era5_data_at_1000hPa = ds_era5.sel(
        level=1000,
        longitude=station_longitude,
        latitude=station_latitude,
        method="nearest",
    )

    df_era5_data_for_station = pd.DataFrame(
        {
            "time": era5_data_at_1000hPa.time.values,
            "pressure_1000": 1000,
            "Humidity_1000": era5_data_at_1000hPa.r,
            "Temperature_1000": era5_data_at_1000hPa.t,
            "WindU_1000": era5_data_at_1000hPa.u,
            "WindV_1000": era5_data_at_1000hPa.v,
        }
    )

    # Create a datetime index for the dataframe containing ERA5 simulated data.
    format_string = "%Y-%m-%d %H:%M:%S"
    df_era5_data_for_station["datetime"] = pd.to_datetime(
        df_era5_data_for_station["time"], format=format_string
    )
    df_era5_data_for_station = df_era5_data_for_station.set_index(
        pd.DatetimeIndex(df_era5_data_for_station["datetime"])
    )
    df_era5_data_for_station = df_era5_data_for_station.drop(
        ["time", "datetime"], axis=1
    )

    # Temperature in ERA5 is provided in Kelvin; convert to Celsius.
    df_era5_data_for_station["Temperature_1000_Celsius"] = df_era5_data_for_station[
        "Temperature_1000"
    ].apply(util.convert_to_celsius)

    assert not df_era5_data_for_station.isnull().values.any().any()

    df_fusion = pd.merge(
        df_station,
        df_era5_data_for_station,
        how="left",
        left_index=True,
        right_index=True,
    )

    column_name_mapping = {
        "Temperature_1000_Celsius": "temperature",
        "Humidity_1000": "relative_humidity",
        "pressure_1000": "barometric_pressure",
        "WindU_1000": "wind_direction_u",
        "WindV_1000": "wind_direction_v",
        "precipitation_sum": "precipitation",
    }
    column_names = column_name_mapping.keys()

    df_fusion = util.get_dataframe_with_selected_columns(df_fusion, column_names)

    df_fusion = util.rename_dataframe_column_names(df_fusion, column_name_mapping)

    print(f"Number of observations (fused gauge station): {df_fusion.shape[0]}")

    return df_fusion


def fuse_rain_gauges_and_era5():
    era5_filename = "./data/NWP/ERA5/RJ_1997_2023.nc"
    ds_era5 = xr.open_dataset(era5_filename)
    time_min = ds_era5.time.min().values
    time_max = ds_era5.time.max().values
    print(f"Range of timestamps in the ERA5 data: [{time_min}, {time_max}]")

    alertario_stations_filename = "./data/ws/alertario_stations.parquet"
    df_alertario_stations = pd.read_parquet(alertario_stations_filename)
    for index, row in df_alertario_stations.iterrows():
        station_id = row["estacao_desc"]
        print(f"Fusing data for gauge station {station_id}")
        station_latitude = row["latitude"]
        station_longitude = row["longitude"]
        df_station = pd.read_parquet(
            "./data/ws/alertario/rain_gauge/" + station_id + ".parquet"
        )
        df_fusion_result = fuse_rain_gauge_and_era5(
            df_station, station_latitude, station_longitude, ds_era5
        )
        assert not df_fusion_result.isnull().values.any().any()
        df_fusion_result.to_parquet(
            "./data/ws/alertario/rain_gauge_era5_fused/" + station_id + ".parquet"
        )


if __name__ == "__main__":
    fuse_rain_gauges_and_era5()
