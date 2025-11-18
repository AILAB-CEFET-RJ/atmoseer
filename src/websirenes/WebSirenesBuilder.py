from pathlib import Path
from typing import Annotated

import pandas as pd
import pandera as pa

from .Logger import logger
from .WebSirenesParser import WebSirenesParser, websirenes_parser

log = logger.get_logger(__name__)


class WebSirenesBuilder:
    def __init__(
        self, websirenes_parser: WebSirenesParser, websirenes_coords: pd.DataFrame
    ) -> None:
        self.websirenes_datasets_path = Path(__file__).parent / "websirenes_datasets"
        self.websirenes_parser = websirenes_parser
        self.websirenes_coords = websirenes_coords

    @property
    def _not_founds_in_coords(
        self,
    ) -> list[dict[Annotated[str, "name"], Annotated[int, "station_id"]]]:
        stations_name_id = []

        for file in self.websirenes_parser.list_files():
            name, station_id = self.websirenes_parser.read_station_name_id_txt_file(
                self.websirenes_parser.websirenes_defesa_civil_path / file
            )
            stations_name_id.append({"name": name, "station_id": station_id})

        not_founds_in_parquet = []
        for station_name_id in stations_name_id:
            name = station_name_id["name"]
            if name not in self.websirenes_coords["estacao"].values:
                not_founds_in_parquet.append(station_name_id)
        return not_founds_in_parquet

    def merge_by_name(
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

    def _create_key(self, df: pd.DataFrame) -> Annotated[str, "lat_long"]:
        row = df.iloc[0]
        return f"{row['latitude']}_{row['longitude']}"

    def _write_dataset_key(self, df: pd.DataFrame, key: str):
        if not self.websirenes_datasets_path.exists():
            self.websirenes_datasets_path.mkdir()
        if (self.websirenes_datasets_path / f"{key}.parquet").exists():
            print(f"Dataset {key}.parquet already exists")
            return
        df.to_parquet(self.websirenes_datasets_path / f"{key}.parquet")

    def build_dataset_keys(self):
        if not self.websirenes_datasets_path.exists():
            self.websirenes_datasets_path.mkdir()
        for i, file in enumerate(self.websirenes_parser.list_files()):
            log.info(
                f"Processing file {i + 1} of {len(self.websirenes_parser.list_files())}"
            )
            df = self.websirenes_parser.get_dataframe(
                self.websirenes_parser.websirenes_defesa_civil_path / file
            )
            station_name = df.nome.iloc[0]
            if station_name in [x["name"] for x in self._not_founds_in_coords]:
                log.info(f"Station {station_name} not found in parquet")
                continue

            df = self.merge_by_name(self.websirenes_coords, df)
            key = self._create_key(df)
            self._write_dataset_key(df, key)
            log.info(f"""
                Station name: {station_name}
                Station key: {key}
                Initial operation date: {df.index.min()}
                Last operation date: {df.index.max()}
            """)


class WebSireneCoordsSchema(pa.DataFrameModel):
    id_estacao: int
    estacao: str
    estacao_desc: str
    latitude: float = pa.Field(ge=-90, le=90, nullable=False)
    longitude: float = pa.Field(ge=-180, le=180, nullable=False)


websirenes_coords = pd.read_parquet(Path(__file__).parent / "websirenes_coords.parquet")
WebSireneCoordsSchema.validate(websirenes_coords)

websirenes_builder = WebSirenesBuilder(websirenes_parser, websirenes_coords)
