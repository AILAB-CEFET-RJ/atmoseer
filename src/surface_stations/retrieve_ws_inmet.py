import argparse
import sys
from datetime import datetime

import pandas as pd

from config import globals


def retrieve_from_station(station_id, beginning_year, end_year, api_token):
    current_date = datetime.now()
    current_year = current_date.year
    end_year = min(end_year, current_year)

    years = list(range(beginning_year, end_year + 1))
    df_inmet_stations = pd.read_json(globals.INMET_API_BASE_URL + "/estacoes/T")
    station_row = df_inmet_stations[df_inmet_stations["CD_ESTACAO"] == station_id]
    df_observations_for_all_years = None
    print(f"Downloading observations from weather station {station_id}...")
    for year in years:
        print(f"Downloading observations for year {year}...")

        if year == end_year:
            datetime_str = str(end_year) + "-12-31"
            last_date_of_the_end_year = datetime.strptime(datetime_str, "%Y-%m-%d")
            end_date = min(last_date_of_the_end_year, current_date)
            end_date_str = end_date.strftime("%Y-%m-%d")
            query_str = (
                globals.INMET_API_BASE_URL
                + "/token/estacao/"
                + str(year)
                + "-01-01/"
                + end_date_str
                + "/"
                + station_id
                + "/"
                + api_token
            )
        else:
            query_str = (
                globals.INMET_API_BASE_URL
                + "/token/estacao/"
                + str(year)
                + "-01-01/"
                + str(year)
                + "-12-31/"
                + station_id
                + "/"
                + api_token
            )

        print(f"Query to be sent to server: {query_str}")

        df_observations_for_a_year = pd.read_json(query_str)
        if df_observations_for_all_years is None:
            temp = [df_observations_for_a_year]
        else:
            temp = [df_observations_for_all_years, df_observations_for_a_year]
        df_observations_for_all_years = pd.concat(temp)
    filename = (
        globals.WS_INMET_DATA_DIR + station_row["CD_ESTACAO"].iloc[0] + ".parquet"
    )
    print(f"Done! Saving dowloaded content to '{filename}'.")
    df_observations_for_all_years.to_parquet(filename)


def retrieve_data(station_id, initial_year, final_year, api_token):
    if station_id == "all":
        df_inmet_stations = pd.read_json(globals.INMET_API_BASE_URL + "/estacoes/T")
        station_row = df_inmet_stations[
            df_inmet_stations["CD_ESTACAO"].isin(globals.INMET_WEATHER_STATION_IDS)
        ]
        for j in list(range(0, len(station_row))):
            station_id = station_row["CD_ESTACAO"].iloc[j]
            retrieve_from_station(station_id, initial_year, final_year, api_token)
    else:
        retrieve_from_station(station_id, initial_year, final_year, api_token)


def main(argv):
    station_id = ""

    parser = argparse.ArgumentParser(
        prog=argv[0],
        usage="{0} -s <station_id> -b <begin_year> -e <end_year> -t <api_token>".format(
            argv[0]
        ),
        description="""This script provides a simple interface for retrieving observations
                                       made by a user-provided weather station from the INMET archive.""",
    )
    parser.add_argument(
        "-t", "--api_token", required=True, help="INMET API token", metavar=""
    )
    parser.add_argument(
        "-s", "--station_id", required=True, help="Weather station ID", metavar=""
    )
    parser.add_argument(
        "-b", "--begin_year", type=int, required=True, help="Start year", metavar=""
    )
    parser.add_argument(
        "-e", "--end_year", type=int, required=True, help="End year", metavar=""
    )

    args = parser.parse_args(argv[1:])

    api_token = args.api_token
    station_id = args.station_id
    start_year = args.begin_year
    end_year = args.end_year

    if station_id not in globals.INMET_WEATHER_STATION_IDS:
        parser.error(f"Invalid station ID: {station_id}")

    assert (api_token is not None) and (api_token != "")
    assert (station_id is not None) and (station_id != "")
    assert start_year <= end_year

    retrieve_data(station_id, start_year, end_year, api_token)


if __name__ == "__main__":
    main(sys.argv)
