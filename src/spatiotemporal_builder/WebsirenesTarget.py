import concurrent.futures
import os
import time
from pathlib import Path
from typing import Optional

import numpy as np
import numpy.typing as npt
import pandas as pd
import xarray as xr
from tqdm import tqdm

from .Logger import TqdmLogger, logger
from .WebSirenesParser import WebSirenesParser, websirenes_parser
from .WebSirenesSquare import WebSirenesSquare, websirenes_square

log = logger.get_logger(__name__)


def get_grid_lats_lons() -> tuple[npt.NDArray[np.float32], npt.NDArray[np.float32]]:
    era5land_path = Path(__file__).parent / "ERA5Land/monthly_data/RJ_2022_1.nc"
    ds = xr.open_dataset(era5land_path)
    lats = ds.coords["latitude"].values
    lons = ds.coords["longitude"].values
    return lats, lons


class WebsirenesTarget:
    def __init__(
        self,
        websirenes_parser: WebSirenesParser,
        websirenes_square: WebSirenesSquare,
    ):
        self.websirenes_target_path = Path(__file__).parent / "target"
        if not self.websirenes_target_path.exists():
            self.websirenes_target_path.mkdir()

        self.websirenes_features_path = Path(__file__).parent / "features"
        if not self.websirenes_features_path.exists():
            self.websirenes_features_path.mkdir()

        self.era5land_path = Path(__file__).parent.parent.parent / "data/reanalysis/ERA5Land"
        if not self.era5land_path.exists():
            log.error(
                f"ERA5Land folder not found in {self.era5land_path}. Please place the ERA5Land dataset in the expected folder"
            )
            exit(1)

        self.current_ds = None
        self.current_year_month = None
        self.websirenes_parser = websirenes_parser
        self.websirenes_square = websirenes_square

        lats, lons = self._get_grid_lats_lons()

        self.sorted_latitudes_ascending = np.sort(lats)
        self.sorted_longitudes_ascending = np.sort(lons)

    def _write_target(self, target: npt.NDArray[np.float64], timestamp: pd.Timestamp):
        target_filename = self.websirenes_target_path / f"{timestamp.strftime('%Y_%m_%d_%H')}.npy"
        np.save(target_filename, target)

    def _write_features(self, features: npt.NDArray[np.float64], timestamp: pd.Timestamp):
        features_filename = (
            self.websirenes_features_path / f"{timestamp.strftime('%Y_%m_%d_%H')}_features.npy"
        )
        np.save(features_filename, features)

    def _get_grid_lats_lons(self) -> tuple[npt.NDArray[np.float32], npt.NDArray[np.float32]]:
        ds = self._get_era5land_dataset(2022, 1)
        lats = ds.coords["latitude"].values
        lons = ds.coords["longitude"].values
        return lats, lons

    def _get_era5land_dataset(self, year: int, month: int) -> xr.Dataset:
        if self.current_year_month == (year, month) and self.current_ds is not None:
            return self.current_ds

        era5land_year_month_path = self.era5land_path / "monthly_data" / f"RJ_{year}_{month}.nc"

        if not os.path.exists(era5land_year_month_path):
            raise FileNotFoundError(f"File {era5land_year_month_path} not found")

        ds = xr.open_dataset(era5land_year_month_path)
        if "expver" in list(ds.coords.keys()):
            log.warning(">>>Oops! expver dimension found. Going to remove it.<<<")
            ds_combine = ds.sel(expver=1).combine_first(ds.sel(expver=5))
            ds_combine.load()
            ds = ds_combine
        ds = ds[["u10", "v10", "d2m", "t2m", "sp", "tp"]]
        self.current_ds = ds
        self.current_year_month = (year, month)
        return self.current_ds

    def _get_relative_humidity(self, temperature: np.float64, dew_point: np.float64):
        return (
            100
            * np.exp((17.625 * dew_point) / (243.04 + dew_point))
            / np.exp((17.625 * temperature) / (243.04 + temperature))
        )

    def _process_grid(
        self,
        features: npt.NDArray[np.float64],
        target: npt.NDArray[np.float64],
        ds_time: xr.Dataset,
        timestamp: pd.Timestamp,
    ):
        top_down_lats = self.sorted_latitudes_ascending[::-1]
        left_right_lons = self.sorted_longitudes_ascending
        for i, lat in enumerate(top_down_lats):
            for j, lon in enumerate(left_right_lons):
                square = self.websirenes_square.get_square(
                    lat, lon, self.sorted_latitudes_ascending, self.sorted_longitudes_ascending
                )

                if square is None:
                    continue

                websirene_keys = self.websirenes_square.get_keys_in_square(square)

                precipitation_in_square = self.websirenes_square.get_precipitation_in_square(
                    square, websirene_keys, timestamp, ds_time
                )

                target[i, j] = precipitation_in_square

                u10, v10, t2m, sp, d2m = self.websirenes_square.get_features_in_square(
                    square, ds_time
                )
                spd10 = np.sqrt(u10**2 + v10**2)
                rh = self._get_relative_humidity(t2m, d2m)

                features[i, j] = [u10, v10, spd10, t2m, sp, rh, precipitation_in_square]

    def _process_timestamp(self, timestamp: pd.Timestamp):
        year = timestamp.year
        month = timestamp.month
        day = timestamp.day
        hour = timestamp.hour

        ds = self._get_era5land_dataset(year, month)

        time = f"{year}-{month}-{day}T{hour}:00:00.000000000"
        ds_time = ds.sel(time=time, method="nearest")
        target = np.zeros(
            (self.sorted_latitudes_ascending.size, self.sorted_longitudes_ascending.size),
            dtype=np.float64,
        )

        features_tuple = {
            "u10": "10 metre U wind component",
            "v10": "10 metre V wind component",
            "spd10": "10 metre wind speed",
            "t2m": "2 metre temperature",
            "sp": "Surface pressure",
            "humidty": "Relative humidity from temp and dew point",
            "tp": "Total precipitation",
        }
        features = np.zeros(
            (
                self.sorted_latitudes_ascending.size,
                self.sorted_longitudes_ascending.size,
                len(features_tuple),
            ),
            dtype=np.float64,
        )

        self._process_grid(features, target, ds_time, timestamp)
        self._write_target(target, timestamp)
        self._write_features(features, timestamp)

    def build_timestamps_hourly(
        self,
        start_date: Optional[pd.Timestamp],
        end_date: Optional[pd.Timestamp],
        use_cache: bool = True,
    ):
        minimum_date = self.websirenes_parser.minimum_date if start_date is None else start_date
        maximum_date = self.websirenes_parser.maximum_date if end_date is None else end_date

        assert minimum_date != pd.Timestamp.max, "minimum_date should be set during keys building"
        assert maximum_date != pd.Timestamp.min, "maximum_date should be set during keys building"

        if use_cache and len(list(self.websirenes_target_path.glob("*.npy"))) > 0:
            log.info(
                f"Using cached target. To clear cache delete the {self.websirenes_target_path} folder"
            )
            return

        timestamps = pd.date_range(start=minimum_date, end=maximum_date, freq="h")

        log.info(f"Building websirenes target from {timestamps[0]} to {timestamps[-1]}")
        start_time = time.time()
        FIVE_MINUTES = 60 * 5
        with concurrent.futures.ProcessPoolExecutor() as executor:
            futures = [
                executor.submit(self._process_timestamp, timestamp) for timestamp in timestamps
            ]
            with tqdm(
                total=len(timestamps),
                desc="Processing timestamps",
                file=TqdmLogger(log),
                dynamic_ncols=True,
                mininterval=FIVE_MINUTES,
            ) as pbar:
                for _ in concurrent.futures.as_completed(futures):
                    pbar.update()
        end_time = time.time()
        log.info(f"Target built in {end_time - start_time:.2f} seconds")  # n 25.22 seconds
        log.success(f"Websirenes target built successfully in {self.websirenes_target_path}")


websirenes_target = WebsirenesTarget(websirenes_parser, websirenes_square)
