import os
from pathlib import Path
from typing import Optional

import numpy as np
import numpy.typing as npt
import pandas as pd
import xarray as xr
from tqdm import tqdm

from .Logger import TqdmLogger, logger
from .WebsirenesTarget import SpatioTemporalFeatures

log = logger.get_logger(__name__)

os.environ["HDF5_USE_FILE_LOCKING"] = "FALSE"


class WebsirenesDataset:
    dataset_path = Path(__file__).parent / "output_dataset.nc"

    def __init__(self, websirenes_target: SpatioTemporalFeatures) -> None:
        self.websirenes_target = websirenes_target
        self.TIMESTEPS = 5

    def _has_timesteps(self, year: int, month: int, day: int, hour: int) -> bool:
        start_time = pd.Timestamp(year=year, month=month, day=day, hour=hour)
        for timestep in reversed(range(self.TIMESTEPS)):
            current_time = start_time - pd.Timedelta(hours=timestep)
            file = (
                self.websirenes_target.features_path
                / f"{current_time.year:04}_{current_time.month:02}_{current_time.day:02}_{current_time.hour:02}_features.npy"
            )
            if not Path(file).exists():
                return False
        return True

    def _get_dataset_with_timesteps(
        self, year: int, month: int, day: int, hour: int, time_step: int = 5
    ) -> npt.NDArray[np.float64]:
        start_time = pd.Timestamp(year=year, month=month, day=day, hour=hour)
        timesteps = []
        oldest_to_newest = reversed(range(time_step))
        for timestep in oldest_to_newest:
            current_time = start_time - pd.Timedelta(hours=timestep)
            file = f"{current_time.year:04}_{current_time.month:02}_{current_time.day:02}_{current_time.hour:02}_features.npy"
            try:
                data = np.load(self.websirenes_target.features_path / file)
            except Exception as e:
                print(f"Error loading file {file}: {e}")
                exit(1)
            timesteps.append(data)
        data = np.stack(timesteps, axis=0)
        return data

    def _process_timestamp(
        self, timestamp: pd.Timestamp, overlapping: bool = True
    ) -> tuple[Optional[npt.NDArray[np.float64]], Optional[npt.NDArray[np.float64]]]:
        year = timestamp.year
        month = timestamp.month
        day = timestamp.day
        hour = timestamp.hour
        if not self._has_timesteps(year, month, day, hour):
            return None, None

        next_timestamp = (
            timestamp + pd.Timedelta(hours=1)
            if overlapping
            else timestamp + pd.Timedelta(hours=self.TIMESTEPS)
        )
        year_y = next_timestamp.year
        month_y = next_timestamp.month
        day_y = next_timestamp.day
        hour_y = next_timestamp.hour

        if not self._has_timesteps(year_y, month_y, day_y, hour_y):
            return None, None

        # log.debug(f"Loading data_x for {year:02}-{month:02}-{day:02} {hour}:00")
        data_x = self._get_dataset_with_timesteps(year, month, day, hour)

        # log.debug(f"Loading data_y for {year:02}-{month:02}-{day:02} {hour + self.TIMESTEPS}:00")
        data_y = self._get_dataset_with_timesteps(year_y, month_y, day_y, hour_y)

        return data_x, data_y

    def build_netcdf(
        self,
        min_timestamp: pd.Timestamp,
        max_timestamp: pd.Timestamp,
        ignored_months: list[int],
        use_cache: bool = True,
        overlapping: bool = True,
    ) -> None:
        if use_cache and self.dataset_path.exists():
            log.warning(
                f"Using cached output_dataset.nc. To clear cache delete the {self.dataset_path} file"
            )
            return

        log.info(f"Building dataset in {self.dataset_path}")

        validated_total_timestamps = self.websirenes_target.validate_timestamps(
            min_timestamp, max_timestamp, ignored_months
        )

        has_last_timestamp_plus_one_hour = self._has_timesteps(
            max_timestamp.year, max_timestamp.month, max_timestamp.day, max_timestamp.hour + 1
        )
        added_extra_hour = 1 if has_last_timestamp_plus_one_hour else 0

        total_samples = (
            validated_total_timestamps - self.TIMESTEPS + added_extra_hour
            if overlapping
            else validated_total_timestamps - (2 * self.TIMESTEPS) + 1 + added_extra_hour
        )

        timestamps = pd.date_range(start=min_timestamp, end=max_timestamp, freq="h")

        log.info(f"""
            Total timestamps: {validated_total_timestamps}
            Min timestamp: {min_timestamp}
            Max timestamp: {max_timestamp}
            Total samples: {total_samples} (overlapping: {overlapping})
        """)

        data_x_list = []
        data_y_list = []
        for timestamp in tqdm(timestamps, mininterval=60, file=TqdmLogger(log)):
            if timestamp.month in ignored_months:
                continue

            data_x, data_y = self._process_timestamp(timestamp, overlapping)
            if data_x is None and data_y is None:
                continue
            data_x_list.append(data_x)
            data_y_list.append(data_y)
            # high space complexity, may need to investigate another approach

        assert len(data_x_list) == len(data_y_list), "Mismatch between data_x and data_y lists"

        data_x = np.stack(data_x_list, axis=0)
        data_y = np.stack(data_y_list, axis=0)

        print(f"data_x shape: {data_x.shape}")
        print(f"data_y shape: {data_y.shape}")

        # assert len(data_x) == total_samples, f"Expected {total_samples} samples, got {len(data_x)}"
        # this assertion is not valid when ignoring months

        assert data_x.shape == data_y.shape, f"x shape {data_x.shape} != y shape {data_y.shape}"

        log.debug(f"data_x shape: {data_x.shape}")
        log.debug(f"data_y shape: {data_y.shape}")

        sample = np.arange(data_x.shape[0])
        timestep = np.arange(data_x.shape[1])
        channel = np.arange(data_x.shape[4])

        ds = xr.Dataset(
            {
                "x": (["sample", "timestep", "lat", "lon", "channel"], data_x),
                "y": (["sample", "timestep", "lat", "lon", "channel"], data_y),
            },
            coords={
                "sample": sample,
                "timestep": timestep,
                "lat": self.websirenes_target.sorted_latitudes_ascending[::-1],
                "lon": self.websirenes_target.sorted_longitudes_ascending,
                "channel": channel,
            },
        )

        ds.to_netcdf(self.dataset_path)
        log.success(f"Dataset saved to {self.dataset_path}")
