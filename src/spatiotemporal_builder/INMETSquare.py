from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

from .ERA5Square import ERA5Square
from .INMETKeys import INMETKeys
from .Logger import logger
from .square import Square

log = logger.get_logger(__name__)


class INMETSquare(ERA5Square):
    def __init__(self, inmet_keys: INMETKeys) -> None:
        self.inmet_keys = inmet_keys

    def get_keys_in_square(
        self, square: Square, stations_inmet: set, verbose: bool = False
    ) -> list[str]:
        keys = [x.stem for x in Path(self.inmet_keys.inmet_keys_path).glob("*.parquet")]
        inmet_keys = []
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

            inmet_keys.append(key)

        if len(inmet_keys) > 0:
            stations_inmet.update(inmet_keys)

        return inmet_keys

    def get_precipitation_in_square(
        self,
        square: Square,
        inmet_keys: list[str],
        timestamp: pd.Timestamp,
        ds_time: xr.Dataset,
    ) -> float:
        if len(inmet_keys) == 0:
            return super().get_era5_single_levels_precipitation_in_square(square, ds_time)
        precipitations: list[float] = []
        for key in inmet_keys:
            df_web = self.inmet_keys.load_key(key)
            df_web_filtered = df_web[df_web.index == timestamp]
            h1 = df_web_filtered["precipitation"]
            if h1.isnull().all():
                h1 = np.array(
                    super().get_era5_single_levels_precipitation_in_square(square, ds_time)
                )
            precipitations.append(h1.item())
        return max(precipitations)
