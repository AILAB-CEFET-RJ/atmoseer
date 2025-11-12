from pathlib import Path

import pandas as pd
import pandera as pa


class INMETCoordsSchema(pa.DataFrameModel):
    id_estacao: str
    estacao: str
    estacao_desc: str
    latitude: float = pa.Field(ge=-90, le=90)
    longitude: float = pa.Field(ge=-180, le=180)


class INMETCoordsSchemaLatLongStr(INMETCoordsSchema):
    latitude: str
    longitude: str


def get_inmet_coords():
    inmet_coords_path = Path(__file__).parent / "../../WeatherStations.csv"

    inmet_coords = pd.read_csv(inmet_coords_path)
    inmet_coords = inmet_coords.rename(
        columns={
            "DC_NOME": "estacao",
            "VL_LATITUDE": "latitude",
            "VL_LONGITUDE": "longitude",
            "STATION_ID": "id_estacao",
            "CD_SITUACAO": "estacao_desc",
        }
    )
    inmet_coords = inmet_coords[["id_estacao", "estacao", "estacao_desc", "latitude", "longitude"]]
    INMETCoordsSchema.validate(inmet_coords)

    inmet_coords["latitude"] = inmet_coords["latitude"].apply(lambda x: str(x))
    inmet_coords["longitude"] = inmet_coords["longitude"].apply(lambda x: str(x))
    inmet_coords["estacao"] = inmet_coords["estacao"].str.strip()
    inmet_coords["id_estacao"] = inmet_coords["id_estacao"].str.strip()

    INMETCoordsSchemaLatLongStr.validate(inmet_coords)

    return inmet_coords
