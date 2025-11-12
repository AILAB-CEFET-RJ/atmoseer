import json
from pathlib import Path
from typing import TypedDict

import pandas as pd
from tqdm import tqdm

from .INMETParser import INMETParser, INMETSchema
from .Logger import logger

log = logger.get_logger(__name__)


class INMETKeySchema(INMETSchema):
    latitude: str
    longitude: str


class StationNameId(TypedDict):
    name: str
    station_id: int


class INMETKeys:
    def __init__(self, inmet_parser: INMETParser, inmet_coords: pd.DataFrame) -> None:
        self.inmet_keys_path = Path(__file__).parent / "inmet_keys"
        if not self.inmet_keys_path.exists():
            self.inmet_keys_path.mkdir()
        self.inmet_parser = inmet_parser
        self.inmet_coords = inmet_coords

    def _not_founds_in_coords(self) -> list[StationNameId]:
        not_founds_in_coords: list[StationNameId] = []
        existing_station_ids = set(self.inmet_coords["id_estacao"].values)
        for file in self.inmet_parser.list_files():
            file_path = str(self.inmet_parser.inmet_path / file)
            station_id = self.inmet_parser.read_station_id(file_path)
            if station_id not in existing_station_ids:
                not_founds_in_coords.append({"name": file, "station_id": station_id})
        if not_founds_in_coords:
            log.warning(f"Stations not found in websirenes coordinates: {not_founds_in_coords}")
        return not_founds_in_coords

    def _merge_by_id(
        self, inmet_coords: pd.DataFrame, inmet_df: pd.DataFrame, station_id: str
    ) -> pd.DataFrame:
        station = inmet_coords[inmet_coords["id_estacao"] == station_id]
        lat = station["latitude"].values[0]
        lon = station["longitude"].values[0]
        inmet_df["latitude"] = lat
        inmet_df["longitude"] = lon
        INMETKeySchema.validate(inmet_df)
        return inmet_df

    def _write_key(self, df: pd.DataFrame):
        row = df.iloc[0]
        assert isinstance(row["latitude"], str), f"{type(row['latitude'])}"
        assert isinstance(row["longitude"], str), f"{type(row['longitude'])}"
        key = f"{row['latitude']}_{row['longitude']}"
        df.to_parquet(self.inmet_keys_path / f"{key}.parquet")

    def load_key(self, key: str) -> pd.DataFrame:
        return pd.read_parquet(f"{self.inmet_keys_path}/{key}.parquet")

    def build_keys(self, use_cache: bool = True):
        total_files = len(list(self.inmet_keys_path.glob("*.parquet")))
        if use_cache and total_files > 0:
            log.warning(
                f"Using cached keys, {total_files} files found. To clear cache delete the {self.inmet_keys_path} folder"
            )
            return

        files = self.inmet_parser.list_files()
        not_found_in_coords = self._not_founds_in_coords()
        log.info(f"Found {len(not_found_in_coords)} stations not found in coordinates")
        log.info(f"Processing {len(files)} files to build keys")
        minimum_date = pd.Timestamp.max
        maximum_date = pd.Timestamp.min
        for file in tqdm(files):
            station_id = file.split("_")[0]

            df = self.inmet_parser.get_dataframe(str(self.inmet_parser.inmet_path / file))

            if df.index.min() < minimum_date:
                minimum_date = df.index.min()
            if df.index.max() > maximum_date:
                maximum_date = df.index.max()

            df = self._merge_by_id(self.inmet_coords, df, station_id)
            self._write_key(df)
        log.info(f"""
            Minimum date: {minimum_date}
            Maximum date: {maximum_date}
        """)
        log.success(f"INMET keys built successfully in {self.inmet_keys_path}")

        minimum_maximum_dates_path = self.inmet_keys_path / "minimum_maximum_dates_inmet.json"
        with open(minimum_maximum_dates_path, "w") as f:
            json.dump(
                {
                    "minimum_date": minimum_date.isoformat(),
                    "maximum_date": maximum_date.isoformat(),
                },
                f,
                indent=4,
            )

        log.success(f"Minimum and maximum dates written to {minimum_maximum_dates_path}")
