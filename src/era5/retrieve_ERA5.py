import argparse
import sys
from collections.abc import Generator
from datetime import datetime
from pathlib import Path

import xarray as xr
from cdsapi.api import Client as ClientCDS
from xarray.core.dataset import Dataset

from config import globals

"""
For using the CDS API to download ERA-5 data consult: https://cds.climate.copernicus.eu/api-how-to
"""

REGION_OF_INTEREST = {"north": -22, "west": -44, "south": -23, "east": -42}


class DatasetClient:
    def __init__(self) -> None:
        self.clientCDS = ClientCDS(timeout=30, retry_max=5, sleep_max=30)

    def _convert_grib_to_netcdf(self, target: str):
        # please see, https://github.com/ecmwf/cfgrib
        # also, https://docs.xarray.dev/en/stable/user-guide/io.html#grib-format-via-cfgrib
        print("Converting grib data to netcdf...")
        assert target.endswith(".grib"), "The target file must be a grib file"
        data = xr.open_dataset(target, engine="cfgrib")
        target = target.replace(".grib", ".nc")
        data.to_netcdf(target)
        print("Converted grib data to netcdf")

    def call_retrieve(self, name: str, request: dict, target: str):
        try:
            self.clientCDS.retrieve(name=name, request=request, target=target)
            print(f"Downloaded ERA5 data - {request['format']} format")
            if request["format"] == "grib":
                self._convert_grib_to_netcdf(target)
        except Exception as e:
            if request["format"] == "grib":
                print(f"Failed to download ERA5 data in grib format. Error {repr(e)}")
                raise e

            print(
                "Failed to download ERA5 data in netcdf format. Downloading in grib format"
            )
            request["format"] = "grib"
            target = target.replace(".nc", ".grib")
            self.call_retrieve(name, request, target)


class CDSDatasetDownloader:
    def __init__(
        self, begin_year: int, begin_month: int, end_year: int, end_month: int
    ) -> None:
        self.begin_year = begin_year
        self.begin_month = begin_month
        self.end_year = min([end_year, datetime.today().year])
        self.end_month = end_month
        self.dataset_client = DatasetClient()

    def _get_dates_generator(self) -> Generator[str, None, None]:
        current_date = datetime.strptime(
            f"{self.begin_year}-{self.begin_month}", "%Y-%m"
        )
        end_date = datetime.strptime(f"{self.end_year}-{self.end_month + 1}", "%Y-%m")
        while current_date < end_date:
            year = int(current_date.year)
            month = int(current_date.month)

            yield year, month

            if month == 12:
                current_date = current_date.replace(year=current_date.year + 1, month=1)
            else:
                current_date = current_date.replace(month=current_date.month + 1)

    def _get_datasets_generator(self) -> Generator[Dataset, None, None]:
        for year, month in self._get_dates_generator():
            yield xr.open_dataset(
                f"{globals.NWP_DATA_DIR}ERA5/montly_data/RJ_{year}_{month}.nc"
            )

    def _download_dataset(self, month: str, year: str):
        target_path_nc = Path(
            f"{globals.NWP_DATA_DIR}ERA5/montly_data/RJ_{year}_{month}.nc"
        )
        if target_path_nc.is_file():
            print(f"ERA5 data already downloaded for the month {month} of year {year}")
            return

        target_path_grib = Path(
            f"{globals.NWP_DATA_DIR}ERA5/montly_data/RJ_{year}_{month}.grib"
        )
        if target_path_grib.is_file():
            print(
                f"ERA5 data already downloaded for the month {month} of year {year} with GRIB format"
            )
            self.dataset_client._convert_grib_to_netcdf(str(target_path_grib.resolve()))
            return

        request = {
            "product_type": "reanalysis",
            "format": "netcdf",
            "variable": [
                "10m_u_component_of_wind",
                "10m_v_component_of_wind",
                "2m_dewpoint_temperature",
                "2m_temperature",
                "convective_available_potential_energy",
                "convective_inhibition",
                "geopotential",
                "k_index",
                "total_totals_index",
            ],
            "year": year,
            "month": [month],
            "day": [f"{day:02d}" for day in range(1, 32)],
            "time": [f"{hour:02d}:00" for hour in range(24)],
            "area": [
                REGION_OF_INTEREST[key] for key in ["north", "west", "south", "east"]
            ],
        }

        print(f"Downloading ERA5 data at month {month} of year {year}...")
        self.dataset_client.call_retrieve(
            name="reanalysis-era5-single-levels",
            request=request,
            target=str(target_path_nc.resolve()),
        )
        print(f"Downloaded ERA5 data at month {month} of year {year}")

    def download_datasets(self):
        print(
            f"Downloading ERA5 data for the period {self.begin_year} to {self.end_year}..."
        )
        for year, month in self._get_dates_generator():
            self._download_dataset(month, year)
        print(
            f"Downloaded ERA5 data for the period {self.begin_year} to {self.end_year}"
        )

    def prepend_dataset(self, prepend_dataset: str):
        if not Path(prepend_dataset).is_file():
            raise FileNotFoundError(f"Dataset to prepend not found: {prepend_dataset}")

        # filename follows this pattern: RJ_YYYY_YYYY.nc
        prepend_dataset_name = Path(prepend_dataset).name
        prepend_begin_year = int(prepend_dataset_name.split("_")[1])
        prepend_end_year = int(prepend_dataset_name.split("_")[-1].split(".")[0])

        target_path = Path(
            f"{globals.NWP_DATA_DIR}ERA5/RJ_{prepend_begin_year}_{self.end_year}.nc"
        )
        if target_path.is_file():
            print(
                f"ERA5 data already prepended for the period {prepend_begin_year} to {self.end_year}"
            )
            return

        print(
            f"""
            Last dataset info:
            Begin year: {prepend_begin_year}
            End year: {prepend_end_year}
        """
        )

        print(
            f"""
            Current CDSDatasetDownloader info:
            Begin year: {self.begin_year}
            End year: {self.end_year}
        """
        )

        assert self.end_year >= prepend_end_year, (
            "The end year must be greater than the last year of the dataset to prepend"
        )

        print(
            f"Prepending ERA5 data {prepend_begin_year}-{prepend_end_year} to {self.begin_year}-{self.end_year}..."
        )

        prepend_ds = xr.open_dataset(prepend_dataset)
        append_ds = xr.open_dataset(
            f"{globals.NWP_DATA_DIR}ERA5/RJ_{self.begin_year}_{self.end_year}.nc"
        )

        ds = prepend_ds.merge(append_ds)
        ds.to_netcdf(str(target_path.resolve()))
        print(
            f"ERA5 data prepended for the period {prepend_begin_year} to {self.end_year}"
        )

    def merge_datasets(self):
        target_path = Path(
            f"{globals.NWP_DATA_DIR}ERA5/RJ_{self.begin_year}_{self.end_year}.nc"
        )
        if target_path.is_file():
            print(
                f"ERA5 data already merged for the period {self.begin_year} to {self.end_year}"
            )
            return

        print(
            f"Merging ERA5 data for the period {self.begin_year} to {self.end_year}..."
        )
        datasets_generator = self._get_datasets_generator()
        ds = next(datasets_generator)
        for dataset in datasets_generator:
            ds = ds.merge(dataset)
        ds.to_netcdf(str(target_path.resolve()))
        print(f"ERA5 data merged for the period {self.begin_year} to {self.end_year}")


def valid_date(arg: str):
    try:
        year_str, month_str = arg.split("-")
        if len(month_str) != 2 or len(year_str) != 4:
            raise ValueError
        year = int(year_str)
        month = int(month_str)
        return year, month
    except ValueError:
        raise argparse.ArgumentTypeError("Invalid date format. Please use YYYY-MM")


def main(argv):
    parser = argparse.ArgumentParser(
        description="Retrieve ERA5 data between two given years."
    )
    parser.add_argument(
        "-b", "--begin", type=valid_date, required=True, help="Begin date (YYYY-MM)"
    )
    parser.add_argument(
        "-e", "--end", type=valid_date, required=True, help="End date (YYYY-MM)"
    )
    parser.add_argument(
        "-pd",
        "--prepend_dataset",
        type=str,
        default=None,
        help="Dataset to merge datasets",
    )
    parser.add_argument(
        "-north",
        "--north",
        type=float,
        default=REGION_OF_INTEREST["north"],
        help="Northernmost latitude",
    )
    parser.add_argument(
        "-west",
        "--west",
        type=float,
        default=REGION_OF_INTEREST["west"],
        help="Westernmost longitude",
    )
    parser.add_argument(
        "-south",
        "--south",
        type=float,
        default=REGION_OF_INTEREST["south"],
        help="Southernmost latitude",
    )
    parser.add_argument(
        "-east",
        "--east",
        type=float,
        default=REGION_OF_INTEREST["east"],
        help="Easternmost longitude",
    )

    args = parser.parse_args(argv[1:])

    begin_year, begin_month = args.begin
    end_year, end_month = args.end
    prepend_dataset = args.prepend_dataset
    REGION_OF_INTEREST["north"] = args.north
    REGION_OF_INTEREST["west"] = args.west
    REGION_OF_INTEREST["south"] = args.south
    REGION_OF_INTEREST["east"] = args.east

    # ERA5 data goes back to the year 1940.
    # see https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-single-levels?tab=form
    assert begin_year >= 1940, "ERA5 start year must be greater than or equal to 1940"
    assert begin_year <= end_year, (
        "ERA5 start year must be less than or equal to end year"
    )

    dataset_downloader = CDSDatasetDownloader(
        begin_year=begin_year,
        begin_month=begin_month,
        end_year=end_year,
        end_month=end_month,
    )
    dataset_downloader.download_datasets()
    dataset_downloader.merge_datasets()
    if prepend_dataset:
        dataset_downloader.prepend_dataset(prepend_dataset)


if __name__ == "__main__":
    main(sys.argv)
