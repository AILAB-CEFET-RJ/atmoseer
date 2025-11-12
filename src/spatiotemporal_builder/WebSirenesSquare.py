from datetime import timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

from .ERA5Square import ERA5Square
from .Logger import logger
from .square import Square
from .WebSirenesKeys import WebSirenesKeys

log = logger.get_logger(__name__)


class WebSirenesSquare(ERA5Square):
    def __init__(self, websirenes_keys: WebSirenesKeys) -> None:
        self.websirenes_keys = websirenes_keys

    def get_keys_in_square(
        self, square: Square, stations_websirenes: set, verbose: bool = False
    ) -> list[str]:
        """
        Get the keys of the websirenes datasets that are inside the square
        Args:
            square (Square): The square to check for keys
        """
        keys = [x.stem for x in Path(self.websirenes_keys.websirenes_keys_path).glob("*.parquet")]
        websirenes_keys = []
        for key in keys:
            key_lat, key_lon = map(float, key.split("_"))

            if (
                verbose
                and not (key_lat < square.bottom_left[0] or key_lat > square.top_left[0])
                and not (key_lon < square.top_left[1] or key_lon > square.top_right[1])
            ):
                log.success(f"""
                    Lat and Lon Square:
                    {square.top_left[0]}{square.top_left[1]} - {square.top_right[0]}{square.top_right[1]}
                    |              {key_lat} {key_lon}                       |
                    {square.bottom_left[0]}{square.bottom_left[1]} - {square.bottom_right[0]}{square.bottom_right[1]}
                """)

            if key_lat < square.bottom_left[0] or key_lat > square.top_left[0]:
                continue
            if key_lon < square.top_left[1] or key_lon > square.top_right[1]:
                continue

            websirenes_keys.append(key)

        if len(websirenes_keys) > 0:
            stations_websirenes.update(websirenes_keys)

        return websirenes_keys

    def get_precipitation_in_square(
        self,
        square: Square,
        websirenes_keys: list[str],
        timestamp: pd.Timestamp,
        ds_time: xr.Dataset,
    ) -> float:
        if len(websirenes_keys) == 0:
            return super().get_era5_single_levels_precipitation_in_square(square, ds_time)

        precipitations_15_min_aggregated: list[float] = []
        for key in websirenes_keys:
            df_web = self.websirenes_keys.load_key(key)

            time_upper_bound = timestamp
            time_lower_bound = timestamp - timedelta(minutes=45)

            df_web_filtered = df_web[
                (df_web.index >= time_lower_bound) & (df_web.index <= time_upper_bound)
            ]

            m15 = df_web_filtered["m15"]
            h01 = df_web[df_web.index == time_upper_bound]["h01"]

            if m15.size < 4 or m15.isnull().any():
                # Websirenes (and also Alertario) have a time resolution of 15 minutes
                # for 16h for example, we'll get four m15 values: 16h, 15h45, 15h30, 15h15
                # to get this data we have two situations:
                # situation 1, the timestamp or the DF index doesn't have all the values:
                # 16h00 = 0.15 mm precipitation
                # 15h45 = 0.05 mm precipitation
                # 15h15 = 0.00 mm precipitation
                # Notice the 15h30 is missing
                # situation 2, the timestamp has all the values, but one of them is NaN:
                # 16h00 = 0.15 mm precipitation
                # 15h45 = 0.05 mm precipitation
                # 15h30 = NaN
                # 15h15 = 0.00 mm precipitation
                # If either of these situations happen:
                # Compare the aggregated sum with the ERA5 data, we use the most significant value
                m15_era5 = super().get_era5_single_levels_precipitation_in_square(square, ds_time)
                m15 = np.array([m15.sum(), m15_era5]).max()
            max_between_m15_and_h01 = np.array([m15.sum().item(), h01.sum().item()]).max()
            precipitations_15_min_aggregated.append(max_between_m15_and_h01.item())

        max_precipitation = max(precipitations_15_min_aggregated)
        # see "ge=0, but txt has -99.99 values" comment in WebSirenesParser.py
        if max_precipitation < 0:
            return super().get_era5_single_levels_precipitation_in_square(square, ds_time)
        return max_precipitation
