import json
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from .AlertarioCoords import AlertarioCoordsSchemaLatLongStr
from .AlertarioParser import AlertarioParser, AlertarioSchema
from .Logger import logger


class AlertarioKeySchema(AlertarioSchema, AlertarioCoordsSchemaLatLongStr):
    pass


log = logger.get_logger(__name__)


class AlertarioKeys:
    def __init__(self, alertario_parser: AlertarioParser, alertario_coords: pd.DataFrame) -> None:
        self.alertario_keys_path = Path(__file__).parent / "alertario_keys"
        if not self.alertario_keys_path.exists():
            self.alertario_keys_path.mkdir()
        self.alertario_describe_path = self.alertario_keys_path / "describe"
        if not self.alertario_describe_path.exists():
            self.alertario_describe_path.mkdir()
        self.alertario_parser = alertario_parser
        self.alertario_coords = alertario_coords

    def _serialize_describe(self, df: pd.DataFrame, describe_path: Path):
        describe = df.describe()
        estacao_Desc = df["estacao_desc"].iloc[0]
        lattiude = df["latitude"].iloc[0]
        longitude = df["longitude"].iloc[0]
        filename = f"{estacao_Desc}_{lattiude}_{longitude}.json"
        describe.to_json(describe_path / filename)

    def _write_key(self, df: pd.DataFrame):
        row = df.iloc[0]
        assert isinstance(row["latitude"], str), f"{type(row['latitude'])}"
        assert isinstance(row["longitude"], str), f"{type(row['longitude'])}"
        key = f"{row['latitude']}_{row['longitude']}"
        df.to_parquet(self.alertario_keys_path / f"{key}.parquet")

    def load_key(self, key: str) -> pd.DataFrame:
        return pd.read_parquet(f"{self.alertario_keys_path}/{key}.parquet")

    def _merge_coords_by_estacao_desc(self, df: pd.DataFrame, estacao_desc: str) -> pd.DataFrame:
        station = self.alertario_coords[self.alertario_coords["estacao_desc"] == estacao_desc]
        lat = station["latitude"].values[0]
        lon = station["longitude"].values[0]
        df["estacao_desc"] = estacao_desc
        df["latitude"] = lat
        df["longitude"] = lon
        AlertarioKeySchema.validate(df)
        return df

    def build_keys(self, use_cache: bool = True) -> None:
        total_files = len(list(self.alertario_keys_path.glob("*.parquet")))
        if use_cache and total_files > 0:
            log.warning(
                f"Using cached keys, {total_files} files found. To clear cache delete the {self.alertario_keys_path} folder"
            )
            return

        minimum_date = pd.Timestamp.max
        maximum_date = pd.Timestamp.min
        stations = self.alertario_parser.list_rain_gauge_stations()
        log.info(f"Processing {len(stations)} files to build keys")
        region_of_interest = self.alertario_parser.get_region_of_interest()
        for station in tqdm(stations):
            data = self.alertario_parser.process_station(station)
            data = self._merge_coords_by_estacao_desc(data, station)

            lat = float(data["latitude"].iloc[0])
            lon = float(data["longitude"].iloc[0])
            if not (
                lat <= region_of_interest["north"]
                and lat >= region_of_interest["south"]
                and lon <= region_of_interest["east"]
                and lon >= region_of_interest["west"]
            ):
                log.error(f"""
                    Station {station} is not in the region of interest:
                    Station lat: {lat}
                    Station lon: {lon}
                    Region of interest: {region_of_interest}
                """)
                exit(1)

            if data["datetime"].min() < minimum_date:
                minimum_date = data["datetime"].min()
            if data["datetime"].max() > maximum_date:
                maximum_date = data["datetime"].max()

            self._write_key(data)

            self._serialize_describe(data, self.alertario_describe_path)

        minimum_maximum_dates_path = (
            self.alertario_keys_path / "minimum_maximum_dates_alertario.json"
        )
        with open(minimum_maximum_dates_path, "w") as f:
            json.dump(
                {
                    "minimum_date": minimum_date.isoformat(),
                    "maximum_date": maximum_date.isoformat(),
                },
                f,
                indent=4,
            )

        log.success(f"""
            Alertario keys built successfully:
            Keys path: {self.alertario_keys_path}
            Describe path: {self.alertario_describe_path}
            Minimum date: {minimum_date}
            Maximum date: {maximum_date}
            Minimum and maximum path: {minimum_maximum_dates_path}
        """)


if __name__ == "__main__":
    # python -m src.spatiotemporal_builder.AlertarioKeys
    from .AlertarioCoords import get_alertario_coords

    alertario_keys = AlertarioKeys(AlertarioParser(), get_alertario_coords())
    alertario_keys.build_keys()
