import os
import re
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Annotated

import numpy as np
import pandas as pd
import pandera as pa
from zoneinfo import ZoneInfo


def get_UTC_offset_from_timezone_name(timezone_name: str) -> str:
    now = datetime.now(ZoneInfo(timezone_name))
    return now.strftime("%z")


class WebSireneSchema(pa.DataFrameModel):
    # horaLeitura: pa.typing.Index[datetime] = pa.Field(coerce=True) # tz naive
    horaLeitura: pa.typing.Index[
        Annotated[
            pd.DatetimeTZDtype,
            "ns",
            f"UTC{get_UTC_offset_from_timezone_name('America/Sao_Paulo')}",
        ]
    ]  # tz aware
    nome: str
    m15: float = pa.Field(nullable=True)
    m30: float = pa.Field(nullable=True)
    h01: float = pa.Field(nullable=True)
    h02: float = pa.Field(nullable=True)
    h03: float = pa.Field(nullable=True)
    h04: float = pa.Field(nullable=True)
    h24: float = pa.Field(nullable=True)
    h96: float = pa.Field(nullable=True)
    station_id: int


class WebSirenesParser:
    def list_files(self) -> list[str]:
        files = os.listdir(Path(__file__).parent / "websirenes_defesa_civil")
        return [file for file in files if file != ".gitignore"]

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
            BARRA DA TIJUCA 3 2021-08-01 00:00:00-03 null 2 3 4 5 6 7 8 ... matches null 2 3 4 5 6 7 8
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
        complete_pattern = self._get_complete_pattern()

        match = re.search(complete_pattern, line)

        name = match.group("name")
        date = match.group("date") + "00"
        timeframe = match.group("timeframe")

        m15, m30, h01, h02, h03, h04, h24, h96, station_id = [
            np.nan
            if x == "null"
            else float(x.replace(",", "."))
            if "," in x
            else float(x)
            for x in timeframe.strip().split()
        ]

        return (
            name,
            datetime.strptime(date, "%Y-%m-%d %H:%M:%S%z"),
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
        file_data: list[tuple] = []
        with open(file_path, "r", encoding="utf-8-sig") as file:
            header = file.readline().strip().split()
            for line in file:
                file_data.append(self._extract_features(line))
        return header, file_data

    def read_station_name_id_txt_file(self, file_path: str) -> tuple[str, int]:
        with open(file_path, "r", encoding="utf-8-sig") as file:
            _ = file.readline().strip().split()
            nome, horaLeitura, m15, m30, h01, h02, h03, h04, h24, h96, station_id = (
                self._extract_features(file.readline())
            )
            return nome, station_id

    def get_dataframe_by_name(self, name: str) -> pd.DataFrame:
        for file in self.list_files():
            nome, _ = self.read_station_name_id_txt_file(
                Path(__file__).parent / "websirenes_defesa_civil" / file
            )
            if nome == name:
                return self.get_dataframe(
                    Path(__file__).parent / "websirenes_defesa_civil" / file
                )
        print(f"Station {name} not found")
        exit(1)

    def get_time_resolution(self, dates: pd.Series) -> dict:
        return dict(Counter([dates[i] - dates[i - 1] for i in range(1, len(dates))]))

    def get_dataframe(self, file_path: str) -> pd.DataFrame:
        header, file_data = self._parse_txt_file(file_path)
        df = pd.DataFrame(file_data, columns=header)
        df.rename(columns={"id": "station_id"}, inplace=True)
        df.set_index("horaLeitura", inplace=True)
        return WebSireneSchema.validate(df)

    def assert_is_sorted_by_date(self, df: pd.DataFrame) -> bool:
        assert df.index.is_monotonic_increasing, "DataFrame index is not sorted by date"


websirenes_parser = WebSirenesParser()
