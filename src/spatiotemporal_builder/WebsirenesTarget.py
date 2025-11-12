import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing.managers import BaseManager
from pathlib import Path
from typing import Optional

import numpy as np
import numpy.typing as npt
import pandas as pd
import xarray as xr
from tqdm import tqdm

from .AlertarioSquare import AlertarioSquare
from .INMETSquare import INMETSquare
from .Logger import TqdmLogger, logger
from .settings import settings
from .square import Square, get_square
from .WebSirenesSquare import WebSirenesSquare

log = logger.get_logger(__name__)


class StationsManager(BaseManager):
    pass


StationsManager.register("Set", set)
StationsManager.register("Dict", dict)


class SpatioTemporalFeatures:
    manager = StationsManager()
    manager.start()
    stations_cells = manager.Set()
    stations_inmet = manager.Set()
    stations_websirenes = manager.Set()
    stations_alertario = manager.Set()
    dataset_era5_year_month = manager.Dict()

    def __init__(
        self,
        websirenes_square: WebSirenesSquare,
        inmet_square: INMETSquare,
        alertario_square: AlertarioSquare,
    ):
        self.features_path = Path(__file__).parent / "features"
        if not self.features_path.exists():
            self.features_path.mkdir()

        self.era5_single_levels_path = (
            Path(__file__).parent.parent.parent / "data/reanalysis/ERA5-single-levels"
        )
        self.era5_pressure_levels_path = (
            Path(__file__).parent.parent.parent / "data/reanalysis/ERA5-pressure-levels"
        )

        self.websirenes_square = websirenes_square
        self.inmet_square = inmet_square
        self.alertario_square = alertario_square

        lats, lons = self._get_grid_lats_lons()

        self.sorted_latitudes_ascending = np.sort(lats)
        self.sorted_longitudes_ascending = np.sort(lons)

        self.features_tuple = {
            "tp": "Total precipitation",
            "r200": "Relative humidity at 200 hPa",
            "r700": "Relative humidity at 700 hPa",
            "r1000": "Relative humidity at 1000 hPa",
            "t200": "Temperature at 200 hPa",
            "t700": "Temperature at 700 hPa",
            "t1000": "Temperature at 1000 hPa",
            "u200": "U component of wind",
            "u700": "U component of wind",
            "u1000": "U component of wind",
            "v200": "V component of wind",
            "v700": "V component of wind",
            "v1000": "V component of wind",
            "speed200": "Speed of wind at 200 hPa",
            "speed700": "Speed of wind at 700 hPa",
            "speed1000": "Speed of wind at 1000 hPa",
            "w200": "Vertical velocity at 200 hPa",
            "w700": "Vertical velocity at 700 hPa",
            "w1000": "Vertical velocity at 1000 hPa",
        }

        log.debug("SpatioTemporalFeatures initialized")
        log.debug(
            f"Grid: {len(self.sorted_latitudes_ascending)}x{len(self.sorted_longitudes_ascending)}"
        )
        log.debug(f"sorted_latitudes_ascending: {self.sorted_latitudes_ascending}")
        log.debug(f"sorted_longitudes_ascending: {self.sorted_longitudes_ascending}")
        log.info(
            f"Spatial resolution: {self.sorted_latitudes_ascending[1] - self.sorted_latitudes_ascending[0]:.2f} degrees"
        )

    def _write_features(self, features: npt.NDArray[np.float64], timestamp: pd.Timestamp):
        features_filename = self.features_path / f"{timestamp.strftime('%Y_%m_%d_%H')}_features.npy"
        np.save(features_filename, features)

    def _get_grid_lats_lons(self) -> tuple[npt.NDArray[np.float32], npt.NDArray[np.float32]]:
        ds = self._get_era5_single_levels_dataset(2009, 6)
        lats = ds.coords["latitude"].values
        lons = ds.coords["longitude"].values
        return lats, lons

    def _get_era5_single_levels_dataset(self, year: int, month: int) -> xr.Dataset:
        if (
            self.dataset_era5_year_month.get((year, month)) == (year, month)
            and self.dataset_era5_year_month.get("single_levels") is not None
        ):
            return self.dataset_era5_year_month.get("single_levels")

        era5_year_month_path = (
            self.era5_single_levels_path / "monthly_data" / f"RJ_{year}_{month}.nc"
        )

        if not os.path.exists(era5_year_month_path):
            raise FileNotFoundError(f"File {era5_year_month_path} not found")

        if self.dataset_era5_year_month.get("single_levels") is not None:
            self.dataset_era5_year_month.get("single_levels").close()

        ds = xr.open_dataset(era5_year_month_path)
        ds = ds[["tp"]]
        self.dataset_era5_year_month.update({(year, month): (year, month)})
        self.dataset_era5_year_month.update({"single_levels": ds})
        return ds

    def _get_era5_pressure_levels_dataset(self, year: int, month: int) -> xr.Dataset:
        if (
            self.dataset_era5_year_month.get((year, month)) == (year, month)
            and self.dataset_era5_year_month.get("pressure_levels") is not None
        ):
            return self.dataset_era5_year_month.get("pressure_levels")

        era5_year_month_path = (
            self.era5_pressure_levels_path / "monthly_data" / f"RJ_{year}_{month}.nc"
        )

        if not era5_year_month_path.exists():
            raise FileNotFoundError(f"File {era5_year_month_path} not found")

        if self.dataset_era5_year_month.get("pressure_levels") is not None:
            self.dataset_era5_year_month.get("pressure_levels").close()

        ds = xr.open_dataset(era5_year_month_path)
        ds = ds[["r", "t", "u", "v", "w"]]
        self.dataset_era5_year_month.update({(year, month): (year, month)})
        self.dataset_era5_year_month.update({"pressure_levels": ds})
        return ds

    def _get_precipitation_in_square(
        self,
        square: Square,
        timestamp: pd.Timestamp,
        ds: xr.Dataset,
        keys: list[tuple],
        lat_index: int,
        lon_index: int,
    ):
        if settings.only_ERA5:
            return self.websirenes_square.get_era5_single_levels_precipitation_in_square(square, ds)

        websirenes_keys = self.websirenes_square.get_keys_in_square(square, keys)

        inmet_keys = self.inmet_square.get_keys_in_square(square, keys)

        alertario_keys = self.alertario_square.get_keys_in_square(square, keys)

        if inmet_keys or websirenes_keys or alertario_keys:
            keys.append((lat_index, lon_index))

        tp_sirenes = self.websirenes_square.get_precipitation_in_square(
            square, websirenes_keys, timestamp, ds
        )

        tp_inmet = self.inmet_square.get_precipitation_in_square(square, inmet_keys, timestamp, ds)

        tp_alertario = self.alertario_square.get_precipitation_in_square(
            square, alertario_keys, timestamp, ds
        )

        return max(tp_sirenes, tp_inmet, tp_alertario)

    def _process_grid(
        self,
        features: npt.NDArray[np.float64],
        ds_single_levels: xr.Dataset,
        ds_pressure_levels: xr.Dataset,
        timestamp: pd.Timestamp,
    ):
        top_down_lats = self.sorted_latitudes_ascending[::-1]
        left_right_lons = self.sorted_longitudes_ascending

        processed = 0
        keys = []
        # O(len(top_down_lats) * len(left_right_lons))
        # O(top_down_lats * left_right_lons * logn)
        for i, lat in enumerate(top_down_lats):
            for j, lon in enumerate(left_right_lons):
                # O(logn), uses bisect
                square = get_square(
                    lat, lon, self.sorted_latitudes_ascending, self.sorted_longitudes_ascending
                )

                if square is None:
                    continue

                tp = self._get_precipitation_in_square(
                    square, timestamp, ds_single_levels, keys, i, j
                )

                # O(4) ~ O(1)
                r1000, r700, r200 = self.websirenes_square.get_relative_humidity_in_square(
                    square, ds_pressure_levels
                )

                # O(4) ~ O(1), all these below are the O(square)
                t1000, t700, t200 = self.websirenes_square.get_temperature_in_square(
                    square, ds_pressure_levels
                )
                u1000, u700, u200 = self.websirenes_square.get_u_component_in_square(
                    square, ds_pressure_levels
                )

                v1000, v700, v200 = self.websirenes_square.get_v_component_in_square(
                    square, ds_pressure_levels
                )
                w1000, w700, w200 = self.websirenes_square.get_w_component_in_square(
                    square, ds_pressure_levels
                )

                speed200 = np.sqrt(u200**2 + v200**2)
                speed700 = np.sqrt(u700**2 + v700**2)
                speed1000 = np.sqrt(u1000**2 + v1000**2)

                features[i, j] = [
                    tp,
                    r200,
                    r700,
                    r1000,
                    t200,
                    t700,
                    t1000,
                    u200,
                    u700,
                    u1000,
                    v200,
                    v700,
                    v1000,
                    speed200,
                    speed700,
                    speed1000,
                    w200,
                    w700,
                    w1000,
                ]
                processed += 1

        self.stations_cells.update(keys)

        total_squares = (len(top_down_lats) - 1) * (len(left_right_lons) - 1)
        assert processed == total_squares, "Not all squares processed"

        bottom_row_pressure_levels = ds_pressure_levels.sel(latitude=min(top_down_lats))
        bottom_row_single_levels = ds_single_levels.sel(latitude=min(top_down_lats))

        right_column_pressure_levels = ds_pressure_levels.sel(longitude=max(left_right_lons))
        right_column_single_levels = ds_single_levels.sel(longitude=max(left_right_lons))

        for j, lon in enumerate(left_right_lons):
            features[-1, j] = [
                bottom_row_single_levels["tp"].values[j] * 1000,
                bottom_row_pressure_levels["r"].sel(pressure_level=200).values[j],
                bottom_row_pressure_levels["r"].sel(pressure_level=700).values[j],
                bottom_row_pressure_levels["r"].sel(pressure_level=1000).values[j],
                bottom_row_pressure_levels["t"].sel(pressure_level=200).values[j],
                bottom_row_pressure_levels["t"].sel(pressure_level=700).values[j],
                bottom_row_pressure_levels["t"].sel(pressure_level=1000).values[j],
                bottom_row_pressure_levels["u"].sel(pressure_level=200).values[j],
                bottom_row_pressure_levels["u"].sel(pressure_level=700).values[j],
                bottom_row_pressure_levels["u"].sel(pressure_level=1000).values[j],
                bottom_row_pressure_levels["v"].sel(pressure_level=200).values[j],
                bottom_row_pressure_levels["v"].sel(pressure_level=700).values[j],
                bottom_row_pressure_levels["v"].sel(pressure_level=1000).values[j],
                np.sqrt(
                    bottom_row_pressure_levels["u"].sel(pressure_level=200).values[j] ** 2
                    + bottom_row_pressure_levels["v"].sel(pressure_level=200).values[j] ** 2
                ),
                np.sqrt(
                    bottom_row_pressure_levels["u"].sel(pressure_level=700).values[j] ** 2
                    + bottom_row_pressure_levels["v"].sel(pressure_level=700).values[j] ** 2
                ),
                np.sqrt(
                    bottom_row_pressure_levels["u"].sel(pressure_level=1000).values[j] ** 2
                    + bottom_row_pressure_levels["v"].sel(pressure_level=1000).values[j] ** 2
                ),
                bottom_row_pressure_levels["w"].sel(pressure_level=200).values[j],
                bottom_row_pressure_levels["w"].sel(pressure_level=700).values[j],
                bottom_row_pressure_levels["w"].sel(pressure_level=1000).values[j],
            ]
            processed += 1

        for i, lat in enumerate(top_down_lats):
            features[i, -1] = [
                right_column_single_levels["tp"].values[i] * 1000,
                right_column_pressure_levels["r"].sel(pressure_level=200).values[i],
                right_column_pressure_levels["r"].sel(pressure_level=700).values[i],
                right_column_pressure_levels["r"].sel(pressure_level=1000).values[i],
                right_column_pressure_levels["t"].sel(pressure_level=200).values[i],
                right_column_pressure_levels["t"].sel(pressure_level=700).values[i],
                right_column_pressure_levels["t"].sel(pressure_level=1000).values[i],
                right_column_pressure_levels["u"].sel(pressure_level=200).values[i],
                right_column_pressure_levels["u"].sel(pressure_level=700).values[i],
                right_column_pressure_levels["u"].sel(pressure_level=1000).values[i],
                right_column_pressure_levels["v"].sel(pressure_level=200).values[i],
                right_column_pressure_levels["v"].sel(pressure_level=700).values[i],
                right_column_pressure_levels["v"].sel(pressure_level=1000).values[i],
                np.sqrt(
                    right_column_pressure_levels["u"].sel(pressure_level=200).values[i] ** 2
                    + right_column_pressure_levels["v"].sel(pressure_level=200).values[i] ** 2
                ),
                np.sqrt(
                    right_column_pressure_levels["u"].sel(pressure_level=700).values[i] ** 2
                    + right_column_pressure_levels["v"].sel(pressure_level=700).values[i] ** 2
                ),
                np.sqrt(
                    right_column_pressure_levels["u"].sel(pressure_level=1000).values[i] ** 2
                    + right_column_pressure_levels["v"].sel(pressure_level=1000).values[i] ** 2
                ),
                right_column_pressure_levels["w"].sel(pressure_level=200).values[i],
                right_column_pressure_levels["w"].sel(pressure_level=700).values[i],
                right_column_pressure_levels["w"].sel(pressure_level=1000).values[i],
            ]
            processed += 1
        # the corner cell is processed twice, is the common point between the last row and the last column
        processed -= 1
        total_squares = len(top_down_lats) * len(left_right_lons)
        assert (
            processed == total_squares
        ), "Not all cells processed failed to include last row and last column"

    def _process_timestamp(self, timestamp: pd.Timestamp):
        year = timestamp.year
        month = timestamp.month
        day = timestamp.day
        hour = timestamp.hour

        time = f"{year}-{month}-{day}T{hour}:00:00.000000000"

        ds_single_levels_month = self._get_era5_single_levels_dataset(year, month)
        ds_pressure_levels_month = self._get_era5_pressure_levels_dataset(year, month)

        ds_single_levels_time = ds_single_levels_month.sel(valid_time=time, method="nearest")
        ds_pressure_levels_time = ds_pressure_levels_month.sel(valid_time=time, method="nearest")

        features = np.zeros(
            (
                self.sorted_latitudes_ascending.size,
                self.sorted_longitudes_ascending.size,
                len(self.features_tuple),
            ),
            dtype=np.float64,
        )

        self._process_grid(features, ds_single_levels_time, ds_pressure_levels_time, timestamp)
        self._write_features(features, timestamp)
        ds_single_levels_time.close()
        ds_pressure_levels_time.close()

    def build_timestamps_hourly(
        self,
        start_date: Optional[pd.Timestamp],
        end_date: Optional[pd.Timestamp],
        ignored_months: list[int],
        use_cache: bool = True,
    ):
        minimum_date = start_date
        maximum_date = end_date

        timestamps = pd.date_range(start=minimum_date, end=maximum_date, freq="h")

        log.info(f"Building websirenes target from {timestamps[0]} to {timestamps[-1]}")
        start_time = time.time()
        ONE_MINUTE = 60 * 1
        all_cached = True

        with ProcessPoolExecutor() as executor:
            futures = []

            for timestamp in timestamps:
                if (
                    use_cache
                    and self.features_path.joinpath(
                        f"{timestamp.strftime('%Y_%m_%d_%H')}_features.npy"
                    ).exists()
                ):
                    continue

                if timestamp.month in ignored_months:
                    continue

                all_cached = False
                futures.append(executor.submit(self._process_timestamp, timestamp))
            log.info(f"Tasks submitted - {len(futures)}")

            with tqdm(
                total=len(timestamps),
                desc="Processing timestamps",
                file=TqdmLogger(log),
                dynamic_ncols=True,
                mininterval=ONE_MINUTE,
            ) as pbar:
                for future in as_completed(futures):
                    try:
                        future.result()
                        pbar.update()
                    except Exception as e:
                        log.error(f"Error processing timestamp: {e}")
                        raise SystemExit(e)

            self.found_stations = self.stations_websirenes._getvalue()
            self.found_stations_inmet = self.stations_inmet._getvalue()
            self.found_stations_alertario = self.stations_alertario._getvalue()
            self.stations_cells = self.stations_cells._getvalue()
            self.manager.shutdown()

        end_time = time.time()
        log.info(f"Target built in {end_time - start_time:.2f} seconds - parallel")

        # start_time = time.time()
        # os.environ["IS_SEQUENTIAL"] = "True"
        # for i in tqdm(
        #     range(len(timestamps)),
        #     desc="Processing timestamps",
        #     file=TqdmLogger(log),
        #     dynamic_ncols=True,
        #     mininterval=ONE_MINUTE,
        # ):
        #     if self.features_path.joinpath(
        #         f"{timestamps[i].strftime('%Y_%m_%d_%H')}_features.npy"
        #     ).exists():
        #         continue

        #     if timestamps[i].month in ignored_months:
        #         continue

        #     self._process_timestamp(timestamps[i])
        # self.found_stations = self.stations_websirenes._getvalue()
        # self.found_stations_inmet = self.stations_inmet._getvalue()
        # self.found_stations_alertario = self.stations_alertario._getvalue()
        # self.stations_cells = self.stations_cells._getvalue()
        # self.manager.shutdown()
        # end_time = time.time()
        # log.info(f"Target built in {end_time - start_time:.2f} seconds - sequential")

        validated_total_timestamps = self.validate_timestamps(
            minimum_date, maximum_date, ignored_months
        )

        log.success(
            f"Websirenes features hourly built successfully in {self.features_path} - {validated_total_timestamps} files"
        )

        assert (
            settings.only_ERA5
            or all_cached
            or len(self.found_stations)
            == len(
                list(self.websirenes_square.websirenes_keys.websirenes_keys_path.glob("*.parquet"))
            )
        ), "Expected all websirenes stations to be found and processed"

        assert (
            settings.only_ERA5
            or all_cached
            or len(list(self.inmet_square.inmet_keys.inmet_keys_path.glob("*.parquet")))
        ), "Expected all inmet stations to be found and processed"

        assert (
            settings.only_ERA5
            or all_cached
            or len(list(self.alertario_square.alertario_keys.alertario_keys_path.glob("*.parquet")))
        ), "Expected all alertario stations to be found and processed"

        if not all_cached and not settings.only_ERA5:
            log.success(f"""
                All stations processed:
                Websirenes: {len(self.found_stations)} files
                INMET: {len(self.found_stations_inmet)} files
                Alertario: {len(self.found_stations_alertario)} files
                Total cells with station: {len(self.stations_cells)}
            """)

        if len(self.stations_cells) > 0:
            np.save(self.features_path / "stations_cells.npy", list(self.stations_cells))
            log.success(
                f"set {self.stations_cells} file created in {self.features_path / 'stations_cells.npy'}"
            )

    def validate_timestamps(
        self, min_timestamp: pd.Timestamp, max_timestamp: pd.Timestamp, ignored_months: list[int]
    ) -> int:
        timestamps = pd.date_range(start=min_timestamp, end=max_timestamp, freq="h")
        not_found = []
        total_timestamps = 0
        total_files = 0

        for timestamp in timestamps:
            if timestamp.month in ignored_months:
                continue

            total_timestamps += 1
            year = timestamp.year
            month = timestamp.month
            day = timestamp.day
            hour = timestamp.hour
            file = self.features_path / f"{year:04}_{month:02}_{day:02}_{hour:02}_features.npy"

            if not Path(file).exists():
                not_found.append(timestamp)
                continue

            features = np.load(file)
            assert (
                features.shape[0] == len(self.sorted_latitudes_ascending)
            ), f"shape[0] should be {len(self.sorted_latitudes_ascending)} but is {features.shape[0]}"
            assert (
                features.shape[1] == len(self.sorted_longitudes_ascending)
            ), f"shape[1] should be {len(self.sorted_longitudes_ascending)} but is {features.shape[1]}"
            assert features.shape[2] == len(
                self.features_tuple
            ), f"shape[2] should be {len(self.features_tuple)} but is {features.shape[2]}"

            assert np.all(
                np.any(features != 0, axis=(1, 2))
            ), f"Should not have one row with all values as zero for {file}"

            total_files += 1

        if not_found:
            log.error(f"Missing timestamps: {not_found}")
            exit(1)

        assert (
            total_files == total_timestamps
        ), "Mismatch between timestamps and files (ignoring specific months)"

        log.success(
            f"""All timestamps found in target directory:
            From={min_timestamp}
            To={max_timestamp}
            Ignoring months={ignored_months}
            Total timestamps={total_timestamps}
            Shape: {features.shape}
            All rows have at least one non-zero value: {np.all(
                np.any(features != 0, axis=(1, 2))
            )}
            """
        )
        return total_timestamps


if __name__ == "__main__":
    ""
