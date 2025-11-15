# -*- coding: utf-8 -*-
"""
retrieve_data_igra.py
----------------------

This script provides a simple interface for retrieving upper air sounding 
observations from the NOAA/NCDC Integrated Global Radiosonde Archive (IGRA v2).

It uses the `igra` Python library to:
 - Download radiosonde data for a given station ID (e.g., BRM00083746),
 - Read and convert them into a pandas DataFrame,
 - Filter by a specified time range,
 - Compute dew point temperature,
 - Save data to a compressed Parquet file.

For an overview of weather balloons, see:
https://www.weather.gov/media/key/Weather-Balloons.pdf
"""

import os
import sys
import argparse
import pandas as pd
from datetime import datetime, timedelta
import time
import requests
import igra
from src.config import globals
import urllib.request



def get_data_for_period(station_id, start_date, end_date):
    """
    Downloads and processes IGRA radiosonde data for the specified station and date range.
    """

    print(f"Downloading IGRA v2 data for {station_id} between {start_date.date()} and {end_date.date()}...")

    # Garante que a pasta de saída exista
    os.makedirs(globals.AS_DATA_DIR, exist_ok=True)

    try:
        # Faz download do arquivo ZIP da estação
        igra.download.station(station_id, globals.AS_DATA_DIR)
        zip_path = os.path.join(globals.AS_DATA_DIR, f"{station_id}-data.txt.zip")

        # Lê os dados baixados
        print("Reading downloaded file...")
        data, station_info = igra.read.igra(station_id, zip_path)
        df = data.to_dataframe().reset_index()
        station_df = station_info.to_dataframe().reset_index()

        # Filtra pelo intervalo de datas informado
        df["date"] = pd.to_datetime(df["date"])
        station_df["date"] = pd.to_datetime(station_df["date"])

        df_filtered = df[(df["date"] >= start_date) & (df["date"] <= end_date)]

        df_final = df_filtered.merge(station_df, on="date", how="left")

        print(f"Loaded {df_final.shape[0]} records (with metadata) from IGRA for {station_id}.")
        return df_final

    except requests.HTTPError as e:
        print(f"HTTP error while downloading IGRA data: {e}")
        time.sleep(10)
        print("Retrying...")
        return get_data_for_period(station_id, start_date, end_date)

    except Exception as e:
        print(f"Unexpected error: {repr(e)}")
        sys.exit(2)


def get_data(station_id, start_date, end_date):
    """
    Orchestrates download, processing, and saving of IGRA radiosonde data.
    """

    # Executa o download e a leitura dos dados
    df_igra = get_data_for_period(station_id, start_date, end_date)

    # Renomeia colunas para compatibilidade
    df_igra = df_igra.rename(columns={
        "pres": "pressure",
        "temp": "temperature",
        "dpd": "dewpoint_depression",
        "date": "time"
    })

    # Converte temperatura para Celsius
    if df_igra["temperature"].max() > 200:
        df_igra["temperature"] = df_igra["temperature"] - 273.15

    # Calcula o ponto de orvalho: T - DPD
    df_igra["dewpoint"] = df_igra["temperature"] - df_igra["dewpoint_depression"]

    # Define o nome do arquivo de saída
    filename = os.path.join(
        globals.AS_DATA_DIR,
        f"{station_id}_{start_date.date()}_{end_date.date()}_igra.parquet.gzip"
    )
 
    # Salva os dados em formato Parquet comprimido (.gzip)
    print(f"Saving {df_igra.shape[0]} observations to {filename}...", end=" ")
    df_igra.to_parquet(filename, compression='gzip', index=False)
    print("Done!")

    return df_igra


def main(argv):
    """
    Entry point for command-line execution.
    """
    # Configura os argumentos que podem ser passados pelo terminal
    parser = argparse.ArgumentParser(
        description="""This script retrieves upper-air sounding data from NOAA's IGRA v2 archive 
        for a specified station ID and time period.""",
        prog=sys.argv[0]
    )
    parser.add_argument('-s', '--station_id', help='IGRA station ID (e.g., BRM00083746)', required=True)
    parser.add_argument('--start_date', help='Start date (YYYY-MM-DD)', required=True)
    parser.add_argument('--end_date', help='End date (YYYY-MM-DD)', required=True)
    args = parser.parse_args()

    # Converte as strings das datas em datetime
    try:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
    except ValueError:
        print("Invalid date format. Use YYYY-MM-DD.")
        sys.exit(2)

    assert end_date >= start_date, "End date must be greater than or equal to start date."

    # Executa todo o processo completo
    get_data(args.station_id, start_date, end_date)
    print("✅ Process completed successfully!")


if __name__ == "__main__":
    # Executa a função principal quando o script é chamado diretamente
    main(sys.argv)
