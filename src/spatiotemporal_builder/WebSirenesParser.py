import os
import re
from pathlib import Path

import numpy as np
import pandas as pd
import pandera as pa

from .Logger import logger

log = logger.get_logger(__name__)


class WebSireneSchema(pa.DataFrameModel):
    horaLeitura: pd.Timestamp
    nome: str
    m15: float = pa.Field(nullable=True)  # ge=0, but txt has -99.99 values
    m30: float = pa.Field(nullable=True)
    h01: float = pa.Field(nullable=True)
    h02: float = pa.Field(nullable=True)
    h03: float = pa.Field(nullable=True)
    h04: float = pa.Field(nullable=True)
    h24: float = pa.Field(nullable=True)
    h96: float = pa.Field(nullable=True)
    station_id: int


class WebSirenesParser:
    websirenes_defesa_civil_path = (
        Path(__file__).parent.parent.parent / "data/ws" / "websirenes_defesa_civil"
    )
    minimum_date = pd.Timestamp.max  # initialize as max to find the minimum date later
    maximum_date = pd.Timestamp.min  # initialize as min to find the maximum date later

    def list_files(self) -> list[str]:
        return os.listdir(self.websirenes_defesa_civil_path)

    def _get_name_pattern(self) -> str:
        """
        Example:
            BARRA DA TIJUCA 3 2021-08-01 00:00:00-03 null 2 ... matches BARRA DA TIJUCA 3
        Returns:
            str: regex pattern to extract name from line
        """
        return r"^(?P<name>.+?)(?=\s+\d{4}-\d{2}-\d{2})"

    def _get_date_pattern(self) -> str:
        """
        Example:
            BARRA DA TIJUCA 3 2021-08-01 00:00:00-03 null 2 ... matches 2021-08-01 00:00:00-03
        Returns:
            str: regex pattern to extract date from line
        """
        return r"(?P<date>\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}-\d{2})"

    def _get_timeframe_pattern(self) -> str:
        """
        Example:
            BARRA DA TIJUCA 3 2021-08-01 00:00:00-03 null 2 3 4 5 6 7 8 ... matches null 2 3 4 5 6 7 8 ...
        Returns:
            str: regex pattern to extract timeframe from line
        """
        return r"(?P<timeframe>.+)$"

    def _get_complete_pattern(self) -> str:
        """
        Example:
            BARRA DA TIJUCA 3 2021-08-01 00:00:00-03 null 2 3 4 5 6 7 8 ...
            matches:
                BARRA DA TIJUCA 3 at group 'name'
                2021-08-01 00:00:00-03 at group 'date'
                null 2 3 4 5 6 7 8 at group 'timeframe'
        Returns:
            str: regex pattern to extract name, date and timeframe from line
        """
        return rf"{self._get_name_pattern()}\s+{self._get_date_pattern()}\s+{self._get_timeframe_pattern()}"

    def _extract_features(self, line: str) -> tuple:
        """
        Extract features from line, the line comes from a txt file with the following format:
            BARRA DA TIJUCA 3 2021-08-01 00:00:00-03 null 2 3 4 5 6 7 8 ...
            Where:
                BARRA DA TIJUCA 3 is the name
                2021-08-01 00:00:00-03 is the date
                null 2 3 4 5 6 7 8 is the timeframe
                ... is the station_id
            Returns:
                tuple: (name, date, m15, m30, h01, h02, h03, h04, h24, h96, station_id)
        """
        complete_pattern = self._get_complete_pattern()

        match = re.search(complete_pattern, line)

        if match is None:
            raise ValueError(f"Could not extract features from line: {line}")

        name = match.group("name")
        date = match.group("date") + "00"
        timeframe = match.group("timeframe")

        m15, m30, h01, h02, h03, h04, h24, h96, station_id = [
            np.nan if x == "null" else float(x.replace(",", ".")) if "," in x else float(x)
            for x in timeframe.strip().split()
        ]

        return (
            name,
            date,
            m15,
            m30,
            h01,
            h02,
            h03,
            h04,
            h24,
            h96,
            int(station_id),
        )

    def _parse_txt_file(self, file_path: str) -> tuple[list[str], list[tuple]]:
        """
        Given a file path, parse the file and return the header and the data
        Returns:
            tuple: (header, data)
            Where:
                header: list of strings with the header of the file
                data: list of tuples with the data of the file
        """
        try:
            file_data: list[tuple] = []
            with open(file_path, "r", encoding="utf-8-sig") as file:
                header = file.readline().strip().split()
                for line in file:
                    file_data.append(self._extract_features(line))
            return header, file_data
        except Exception as e:
            log.error(f"Error parsing file {file_path}: {e}")
            raise e

    def read_station_name_id_txt_file(self, file_path: str) -> tuple[str, int]:
        try:
            with open(file_path, "r", encoding="utf-8-sig") as file:
                _header = file.readline().strip().split()
                (
                    nome,
                    horaLeitura,
                    m15,
                    m30,
                    h01,
                    h02,
                    h03,
                    h04,
                    h24,
                    h96,
                    station_id,
                ) = self._extract_features(file.readline())
                return nome, station_id
        except Exception as e:
            log.error(f"Error parsing file {file_path}: {e}")
            raise e

    def get_dataframe(self, file_path: str) -> pd.DataFrame:
        header, file_data = self._parse_txt_file(file_path)
        df = pd.DataFrame(file_data, columns=header)
        df["horaLeitura"] = pd.to_datetime(
            df["horaLeitura"], format="%Y-%m-%d %H:%M:%S%z"
        ).dt.tz_convert(None)
        df.rename(columns={"id": "station_id"}, inplace=True)
        WebSireneSchema.validate(df)
        df.set_index("horaLeitura", inplace=True)
        return df


websirenes_parser = WebSirenesParser()
