import argparse
from collections.abc import Generator
from datetime import datetime
from pathlib import Path

import pandas as pd
import xarray as xr
from zoneinfo import ZoneInfo

from .check_requirements import check_requirements
from .get_nearest_ERA5Land import get_nearest_ERA5Land
from .Logger import logger
from .plot_values import plot_tp_values
from .WebSirenesBuilder import websirenes_builder, websirenes_coords
from .WebSirenesParser import websirenes_parser

log = logger.get_logger(__name__)


def _get_datasets_generator(
    era5land_nc_files: list[str],
) -> Generator[xr.Dataset, None, None]:
    for nc_file in era5land_nc_files:
        yield xr.open_dataset(
            Path(__file__).parent / "ERA5Land" / "montly_data" / nc_file
        )


def _get_era5land_data(
    nearest_era5land: tuple[float, float],
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> pd.DataFrame:
    start_year, start_month = start_date.year, start_date.month
    end_year, end_month = end_date.year, end_date.month

    era5land_monthly_data_path = Path(__file__).parent / "ERA5Land" / "montly_data"
    if not era5land_monthly_data_path.exists():
        era5land_monthly_data_path = Path(__file__).parent / "ERA5Land" / "monthly_data"
        if not era5land_monthly_data_path.exists():
            log.error(
                f"ERA5Land monthly data path {era5land_monthly_data_path} does not exist."
            )
            exit(1)

    era5land_nc_files = [
        f"RJ_{year}_{month}.nc"
        for year in range(start_year, end_year + 1)
        for month in range(start_month, end_month + 1)
    ]

    datasets_generator = _get_datasets_generator(era5land_nc_files)
    ds = next(datasets_generator)
    for dataset in datasets_generator:
        if "expver" in list(ds.coords.keys()):
            log.warning("expver dimension found. Going to remove it.")
            ds_combine = ds.sel(expver=1).combine_first(ds.sel(expver=5))
            ds_combine.load()
            ds = ds_combine
        ds = ds.merge(dataset)

    nearest_era5land_lat, nearest_era5land_lon = nearest_era5land

    ds = ds.sel(
        latitude=nearest_era5land_lat, longitude=nearest_era5land_lon, method="nearest"
    )
    log.info("ERA5Land ds data:")
    print(ds)

    era5land_df = ds.to_dataframe()
    log.info("ERA5Land df data:")
    print(era5land_df)
    log.info("ERA5Land data summary:")
    print(era5land_df.describe())
    log.info("ERA5Land data time range:")
    print(era5land_df.index.min(), era5land_df.index.max())
    log.info("NaN values in ERA5Land data per column:")
    print(era5land_df.isna().sum())
    return era5land_df


def main(
    start_date: str,
    end_date: str,
    station_name: str,
):
    df = get_websinere(station_name)
    log.info(f"Filtering Websirene data between {start_date} and {end_date}")

    start_date = pd.to_datetime(
        datetime.strptime(start_date, "%Y-%m-%d").replace(
            tzinfo=ZoneInfo("America/Sao_Paulo")
        )
    )
    end_date = pd.to_datetime(
        datetime.strptime(end_date, "%Y-%m-%d").replace(
            tzinfo=ZoneInfo("America/Sao_Paulo")
        )
    )

    if start_date < df.index.min() or start_date > df.index.max():
        log.error(f"Invalid start date {start_date} for WebSirene {station_name} data")
        log.error(f"Data range is {df.index.min()} to {df.index.max()}")
        exit(1)

    if end_date > df.index.max() or end_date < df.index.min():
        log.error(f"Invalid end date {end_date} for WebSirene {station_name} data")
        log.error(f"Data range is {df.index.min()} to {df.index.max()}")
        exit(1)

    df = df.loc[start_date:end_date]
    log.info(f"WebSirene {station_name} data between {start_date} and {end_date}:")
    print(df)
    log.info(f"WebSirene {station_name} data summary:")
    print(df.describe())
    # log.info("WebSirene data time resolution:")
    # print(websirenes_parser.get_time_resolution(df.index))
    log.info("WebSirene data time range:")
    print(df.index.min(), df.index.max())
    log.info("NaN values in WebSirene data per column:")
    print(df.isna().sum())
    log.info("Merging WebSirene data with WebSirene coords")
    df = websirenes_builder.merge_by_name(websirenes_coords, df)
    log.info("WebSirene data after merge:")
    print(df)
    lat, long = df["latitude"].values[0], df["longitude"].values[0]
    log.info(f"Getting nearest ERA5Land station to WebSirene {station_name} station")
    nearest_era5land = get_nearest_ERA5Land(lat, long)
    log.info(f"Nearest ERA5Land station to WebSirene {station_name} station coords:")
    print(nearest_era5land)
    log.info("Getting ERA5Land data...")
    era5land_df = _get_era5land_data(nearest_era5land, start_date, end_date)

    df_m15_values = df[["m15"]]
    log.info("WebSirene m15 and horaLeitura data:")
    print(df_m15_values)
    era5land_df_total_precipitation = era5land_df[["tp"]]
    log.info("ERA5Land total precipitation and time data:")
    print(era5land_df_total_precipitation)
    plot_tp_values(
        df_m15_values,
        era5land_df_total_precipitation,
        start_date,
        end_date,
        station_name,
    )


def show_websirenes_coords_and_names():
    websirenes_coords.to_csv(
        Path(__file__).parent / "websirenes_coords.csv", index=False
    )
    log.info("Websirenes coords stored into csv 'websirenes_coords.csv' file")
    log.info("Websirenes coords data:")
    print(websirenes_coords)
    log.info("Websirenes coords and names:")
    coords_data = [
        {
            "name": row["estacao"],
            "latitude": row["latitude"],
            "longitude": row["longitude"],
        }
        for name, row in websirenes_coords.iterrows()
    ]
    print(coords_data)


def get_websinere(station_name: str) -> pd.DataFrame:
    websirene = websirenes_coords[websirenes_coords["estacao"] == station_name]
    log.info(f"Websirene {station_name} station coords data:")
    print(websirene)
    log.info(f"Getting WebSirene {station_name} data...")
    df = websirenes_parser.get_dataframe_by_name(station_name)
    log.info(f"WebSirene {station_name} data:")
    print(df)
    log.info(f"WebSirene {station_name} data summary:")
    print(df.describe())
    # log.info("WebSirene data time resolution:")
    # print(websirenes_parser.get_time_resolution(df.index))
    log.info("WebSirene data time range:")
    print(df.index.min(), df.index.max())
    log.info("NaN values in WebSirene data per column:")
    print(df.isna().sum())
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Plot ERA5Land total precipitation and websirenes precipitation data between two dates"
    )
    parser.add_argument(
        "--start_date", type=str, help="Start date of the period to be analyzed"
    )
    parser.add_argument(
        "--end_date", type=str, help="End date of the period to be analyzed"
    )
    parser.add_argument("--station_name", type=str, help="Region of interest")

    parser.add_argument(
        "--show_websirenes", action="store_true", help="Show websirenes data"
    )
    parser.add_argument("--get_websirene", type=str, help="Get websirene data")

    args = parser.parse_args()

    check_requirements()

    if args.show_websirenes:
        show_websirenes_coords_and_names()
        exit(0)

    if args.get_websirene:
        get_websinere(args.get_websirene)
        exit(0)

    if not args.start_date or not args.end_date or not args.station_name:
        log.error("Please provide --start_date, --end_date and --station_name")
        exit(1)

    main(
        start_date=args.start_date,
        end_date=args.end_date,
        station_name=args.station_name,
    )
