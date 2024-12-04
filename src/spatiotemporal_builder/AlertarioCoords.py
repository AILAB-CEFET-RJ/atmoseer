from pathlib import Path

import pandas as pd
import pandera as pa


class AlertarioCoordsSchema(pa.DataFrameModel):
    estacao_desc: str
    latitude: float = pa.Field(ge=-90, le=90)
    longitude: float = pa.Field(ge=-180, le=180)


class AlertarioCoordsSchemaLatLongStr(AlertarioCoordsSchema):
    latitude: str
    longitude: str


def get_alertario_coords():
    alertario_coords_path = Path(__file__).parent / "alertario_stations.parquet"

    alertario_coords = pd.read_parquet(alertario_coords_path)
    alertario_coords = alertario_coords[["estacao_desc", "latitude", "longitude"]]
    AlertarioCoordsSchema.validate(alertario_coords)

    alertario_coords["latitude"] = alertario_coords["latitude"].apply(lambda x: str(x))
    alertario_coords["longitude"] = alertario_coords["longitude"].apply(lambda x: str(x))
    alertario_coords["estacao_desc"] = alertario_coords["estacao_desc"].str.strip()

    AlertarioCoordsSchemaLatLongStr.validate(alertario_coords)

    return alertario_coords
