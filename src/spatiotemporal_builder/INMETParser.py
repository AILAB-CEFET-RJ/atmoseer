import os

import pandas as pd


class INMETParser:
    def _format_time(self, input_str: str):
        input_int = int(input_str)
        hours = input_int // 100
        minutes = input_int % 100
        output_str = f"{hours:02}:{minutes:02}"
        return output_str

    def _add_datetime_index(self, df: pd.DataFrame):
        df.HR_MEDICAO = df.HR_MEDICAO.apply(self._format_time)  # e.g., 1800 --> 18:00
        timestamp = pd.to_datetime(df.DT_MEDICAO + " " + df.HR_MEDICAO)
        assert timestamp is not None
        df = df.set_index(pd.DatetimeIndex(timestamp))
        return df

    def _get_dataframe_with_selected_columns(df: pd.DataFrame, column_names: dict):
        selected_columns = []
        for column_name in column_names:
            if column_name in df.columns:
                selected_columns.append(column_name)
            else:
                print(f"The column name {column_name} does not exist in the DataFrame.")
        return df[selected_columns]

    def _rename_dataframe_column_names(df: pd.DataFrame, column_name_mapping: dict):
        new_column_names = []
        for old_column_name, new_column_name in column_name_mapping.items():
            if old_column_name in df.columns:
                new_column_names.append(new_column_name)
            else:
                print(
                    f"The column name {old_column_name} does not exist in the DataFrame."
                )
        df.columns = new_column_names
        return df

    def list_files(self) -> list[str]:
        files = os.listdir("inmet_ws")
        return [
            file
            for file in files
            if file != "A602_preprocessed.parquet.gzip" and file != "A602.old.parquet"
        ]

    def get_station_id_from_filename(self, file_name: str) -> str:
        return file_name.split(".")[0]

    def preprocess(self, df: pd.DataFrame, station_id: str) -> pd.DataFrame:
        df = self._add_datetime_index(df)

        column_name_mapping = {
            "DT_MEDICAO": "datetime",
            "TEM_MAX": "temperature",
            "UMD_MAX": "relative_humidity",
            "PRE_MAX": "barometric_pressure",
            "VEN_VEL": "wind_speed",
            "VEN_DIR": "wind_dir",
            "CHUVA": "precipitation",
        }

        column_names = column_name_mapping.keys()

        df = self._get_dataframe_with_selected_columns(df, column_names)
        df = self._rename_dataframe_column_names(df, column_name_mapping)

        df["station_id"] = station_id
        df.reset_index(inplace=True)
        df.rename(columns={"index": "datetime"}, inplace=True)
        return df


inmet_parser = INMETParser()
