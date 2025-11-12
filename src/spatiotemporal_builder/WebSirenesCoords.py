from pathlib import Path

import pandas as pd
import pandera as pa


class WebSireneCoordsSchema(pa.DataFrameModel):
    id_estacao: int
    estacao: str
    estacao_desc: str
    latitude: float = pa.Field(ge=-90, le=90)
    longitude: float = pa.Field(ge=-180, le=180)


class WebSireneCoordsSchemaLatLongStr(WebSireneCoordsSchema):
    latitude: str
    longitude: str


def get_websirenes_coords() -> pd.DataFrame:
    websirenes_coords_path = Path(__file__).parent / "websirenes_coords.parquet"

    websirenes_coords = pd.read_parquet(websirenes_coords_path)
    WebSireneCoordsSchema.validate(websirenes_coords)

    websirenes_coords["latitude"] = websirenes_coords["latitude"].apply(lambda x: str(x))
    websirenes_coords["longitude"] = websirenes_coords["longitude"].apply(lambda x: str(x))
    websirenes_coords["estacao"] = websirenes_coords["estacao"].str.strip()

    WebSireneCoordsSchemaLatLongStr.validate(websirenes_coords)

    return websirenes_coords
