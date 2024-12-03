import re
from pathlib import Path

import pandas as pd
import pandera as pa
import xarray as xr
from sklearn.impute import KNNImputer

from .Logger import logger

log = logger.get_logger(__name__)


class AlertarioSchema(pa.DataFrameModel):
    datetime: pd.Timestamp
    m15: float = pa.Field(nullable=False, ge=0)
    h01: float = pa.Field(nullable=False, ge=0)


class AlertarioParser:
    rain_gauge_path = Path(__file__).parent / "alertario-from-source"

    def get_region_of_interest(self) -> dict:
        ds = xr.open_dataset("./data/reanalysis/ERA5-single-levels/monthly_data/RJ_2018_1.nc")
        lats = ds.latitude.values
        lons = ds.longitude.values
        region_of_interest = {
            "north": lats.max(),
            "south": lats.min(),
            "east": lons.max(),
            "west": lons.min(),
        }
        return region_of_interest

    def _impute_missing_values(self, df: pd.DataFrame, column: str) -> pd.DataFrame:
        total_values = df.shape[0]
        missing_values = df[column].isna().sum()
        percentage_missing = (missing_values / total_values) * 100

        if percentage_missing == 0:
            return df

        if percentage_missing > 10:
            log.error(
                f"Percentage of missing values for {column} in the region of interest: {percentage_missing:.2f}% ({missing_values}/{total_values}) greater than 10% - exiting"
            )
            exit(1)
        elif percentage_missing > 5:
            log.warning(
                f"Missing values for {column}: {percentage_missing:.2f}% ({missing_values}/{total_values})"
            )

        imputer = KNNImputer(n_neighbors=2)
        df[column] = imputer.fit_transform(df[[column]])
        assert df[column].isna().sum() == 0, "Missing values after imputation should be zero"
        return df

    def _get_df(self, file_path: Path) -> pd.DataFrame:
        df = pd.read_csv(
            file_path,
            sep=r"\s{2,}",
            engine="python",
            skiprows=5,
            names=["Dia", "Hora", "HBV", "m15", "h01", "h04", "h24", "h96"],
        )
        df["datetime"] = pd.to_datetime(df["Dia"] + " " + df["Hora"], format="%d/%m/%Y %H:%M:%S")

        df["m15"] = pd.to_numeric(df["m15"], errors="coerce")
        df["h01"] = pd.to_numeric(df["h01"], errors="coerce")
        df = df.drop(columns=["Dia", "Hora", "HBV", "h04", "h24", "h96"])
        return df

    def process_station(self, station: str):
        station_dfs = []
        months = pd.date_range(pd.Timestamp("2013-01-01"), pd.Timestamp("2024-10-01"), freq="MS")
        for month in months:
            current_year = month.year
            current_month = month.month
            try:
                file_name = f"{station}_{current_year:04d}{current_month:02d}_Plv.txt"
                file_path = self.rain_gauge_path / file_name
                if not file_path.exists():
                    raise FileNotFoundError(f"File {file_path} not found")
                df = self._get_df(file_path)
                station_dfs.append(df)
            except Exception as e:
                print(f"Error processing station {station} at {current_year}-{current_month}: {e}")
                raise e
        assert len(station_dfs) == len(months)
        df = pd.concat(station_dfs).sort_values(by="datetime").reset_index(drop=True)
        df = self._impute_missing_values(df, AlertarioSchema.m15)
        df = self._impute_missing_values(df, AlertarioSchema.h01)
        AlertarioSchema.validate(df)
        return df

    def list_rain_gauge_stations(self) -> list[str]:
        unique_names = set()
        pattern = re.compile(r"^(.*?)_\d{6}_Plv\.txt$")
        for file in self.rain_gauge_path.iterdir():
            filename = file.name
            match = pattern.match(filename)
            if not match:
                raise ValueError(f"Filename {filename} does not match the pattern")
            unique_names.add(match.group(1))
        return sorted(unique_names)


if __name__ == "__main__":
    # python -m src.spatiotemporal_builder.AlertarioParser
    alertario_parser = AlertarioParser()
    for station in alertario_parser.list_rain_gauge_stations():
        print(f"station: {station}")
        data = alertario_parser.process_station(station)
        print(data)
        break
