from pathlib import Path

import pandas as pd
import pandera as pa

from .Logger import logger

log = logger.get_logger(__name__)


class INMETSchema(pa.DataFrameModel):
    precipitation: float = pa.Field(nullable=False, ge=0)


class INMETParser:
    inmet_path = Path(__file__).parent.parent.parent / "data/ws/inmet"

    def list_files(self) -> list[str]:
        return [x.name for x in self.inmet_path.glob("*.gzip")]

    def read_station_id(self, file_path: str) -> str:
        station_id = file_path.split("/")[-1].split("_")[0]
        return station_id

    def get_dataframe(self, file_path: str) -> pd.DataFrame:
        df = pd.read_parquet(file_path)
        # df["horaLeitura"] = pd.to_datetime(
        #     df["horaLeitura"], format="%Y-%m-%d %H:%M:%S%z"
        # ).dt.tz_convert(None)
        INMETSchema.validate(df)
        return df
