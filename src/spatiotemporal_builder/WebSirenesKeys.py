from pathlib import Path
from typing import Optional, TypedDict

import pandas as pd
from tqdm import tqdm

from .Logger import logger
from .WebSirenesCoords import websirenes_coords
from .WebSirenesParser import WebSirenesParser, websirenes_parser

log = logger.get_logger(__name__)


class StationNameId(TypedDict):
    name: str
    station_id: int


class WebSirenesKeys:
    def __init__(
        self, websirenes_parser: WebSirenesParser, websirenes_coords: pd.DataFrame
    ) -> None:
        self.websirenes_keys_path = Path(__file__).parent / "websirenes_keys"
        if not self.websirenes_keys_path.exists():
            self.websirenes_keys_path.mkdir()
        self.websirenes_parser = websirenes_parser
        self.websirenes_coords = websirenes_coords

    def _not_founds_in_coords(self) -> list[StationNameId]:
        not_founds_in_coords: list[StationNameId] = []
        existing_station_names = set(self.websirenes_coords["estacao"].values)
        for file in self.websirenes_parser.list_files():
            file_path = str(self.websirenes_parser.websirenes_defesa_civil_path / file)
            name, station_id = self.websirenes_parser.read_station_name_id_txt_file(file_path)
            if name not in existing_station_names:
                not_founds_in_coords.append({"name": name, "station_id": station_id})
        log.warning(f"Stations not found in websirenes coordinates: {not_founds_in_coords}")
        return not_founds_in_coords

    def _merge_by_name(
        self, websirenes_coords: pd.DataFrame, websirenes_defesa_civil: pd.DataFrame
    ) -> pd.DataFrame:
        websirenes_defesa_civil.reset_index(inplace=True)
        df = pd.merge(
            websirenes_coords,
            websirenes_defesa_civil,
            left_on="estacao",
            right_on="nome",
            how="inner",
        )
        df.drop(columns=["estacao", "id_estacao"], inplace=True)
        df.set_index("horaLeitura", inplace=True)
        return df

    def _write_key(self, df: pd.DataFrame):
        row = df.iloc[0]
        assert isinstance(row["latitude"], str), f"{type(row['latitude'])}"
        assert isinstance(row["longitude"], str), f"{type(row['longitude'])}"
        key = f"{row['latitude']}_{row['longitude']}"
        df.to_parquet(self.websirenes_keys_path / f"{key}.parquet")

    def load_key(self, key: str) -> pd.DataFrame:
        return pd.read_parquet(f"{self.websirenes_keys_path}/{key}.parquet")

    def _set_minimum_date(self, df: pd.DataFrame):
        if df.index.min() < self.websirenes_parser.minimum_date:
            self.websirenes_parser.minimum_date = df.index.min()

    def _set_maximum_date(self, df: pd.DataFrame):
        if df.index.max() > self.websirenes_parser.maximum_date:
            self.websirenes_parser.maximum_date = df.index.max()

    def build_keys(
        self,
        start_date: Optional[pd.Timestamp],
        end_date: Optional[pd.Timestamp],
        use_cache: bool = True,
    ):
        """
        Builds datasets by key (latitude and longitude) for each station

        Each station is mapped to a "lat_lon.parquet" file in the "websirenes_keys" folder
        The keys generated are used to identify whether there are stations in an ERA5land gridpoint

        As a side effect, this function updates the minimum and maximum dates of the dataset as the files are processed

        The function performs the following steps:
        1. Lists all files to process
        2. Reads each file into a DataFrame
        3. Filters out files based on whether the station name is found in the coordinates list
        4. Updates the minimum and maximum dates of the dataset
        5. Merges the DataFrame with existing coordinates
        6. Writes the resulting each DataFrame to a key file

        Returns:
            None
        """

        if (
            use_cache
            and start_date is not None
            and end_date is not None
            and len(list(self.websirenes_keys_path.glob("*.parquet"))) > 0
        ):
            log.info(
                f"Using cached keys. To clear cache delete the {self.websirenes_keys_path} folder"
            )
            return

        files = self.websirenes_parser.list_files()
        not_found_in_coords = self._not_founds_in_coords()
        log.info(f"Processing {len(files)} files to build keys")
        for file in tqdm(files):
            df = self.websirenes_parser.get_dataframe(
                str(self.websirenes_parser.websirenes_defesa_civil_path / file)
            )
            station_name = df.nome.iloc[0]
            if station_name in [x["name"] for x in not_found_in_coords]:
                continue

            self._set_minimum_date(df)
            self._set_maximum_date(df)

            df = self._merge_by_name(self.websirenes_coords, df)
            self._write_key(df)
        log.info(f"""
            Minimum date: {self.websirenes_parser.minimum_date}
            Maximum date: {self.websirenes_parser.maximum_date}
        """)
        log.success(f"Websirenes keys built successfully in {self.websirenes_keys_path}")


websirenes_keys = WebSirenesKeys(websirenes_parser, websirenes_coords)
