"""
This script provides a simple interface for retrieving upper air sounding
observations from the University of Wyoming Upper Air Archive.

Its code uses the WyomingUpperAir class in the siphon library. In particular,
the request_data method of the WyomingUpperAir class takes two arguments:
 - the date of the sounding launch as a datetime object,
 - the atmospheric sounding station ID (as a string).

For an overview of weather baloons, see https://www.weather.gov/media/key/Weather-Balloons.pdf
"""

import argparse
import sys
import time
from datetime import datetime, timedelta

import pandas as pd
import requests
from siphon.simplewebservice.wyoming import WyomingUpperAir

import src.utils.util as util
from config import globals


def get_data_for_year_and_hour_of_day(station_id, first_day, last_day, hour_of_day):
    unsuccesfull_launchs = 0
    next_day = first_day
    df_all_launchs = pd.DataFrame()

    while next_day <= last_day:
        current_launch_time = next_day + timedelta(hours=hour_of_day)
        try:
            print(f"Downloading observations made on {current_launch_time}...", end="")
            df_launch = WyomingUpperAir.request_data(current_launch_time, station_id)
            print(f"Success! ({len(df_launch)} observations).")
            df_all_launchs = pd.concat(([df_all_launchs, df_launch]))
        except IndexError as e:
            print(f"{repr(e)}")
            unsuccesfull_launchs += 1
        except ValueError as e:
            print(f"{str(e)}")
            unsuccesfull_launchs += 1
        except requests.HTTPError as e:
            print(f"{repr(e)}")
            print(
                f"Server seems to be busy while trying to download data for {current_launch_time}. Going to sleep for a while...",
                end="",
            )
            time.sleep(10)  # Sleep for a moment
            print("Back to work!")
            continue
        except Exception as e:
            print(f"Unexpected error! {repr(e)}")
            sys.exit(2)
        # Now, go to next day
        next_day = next_day + timedelta(days=1)
        # nb_launchs += 1
        # if nb_launchs > 10:
        #     break
        # print(f"next_day = {next_day}; last_day = {last_day}")

    return df_all_launchs, unsuccesfull_launchs


def get_data(station_id, start_year, end_year):
    print(f"Downloading observations from sounding station {station_id}...")
    df_all_launchs = pd.DataFrame()
    unsuccesfull_launchs = 0
    end_year = min([end_year, datetime.now().year])

    assert end_year >= start_year

    for year in range(start_year, end_year + 1):
        first_day, last_day = util.get_first_and_last_days_of_year(year)

        # Download data at 00Z
        df_launchs, nb_unsuccessful_launchs_of_year = get_data_for_year_and_hour_of_day(
            station_id, first_day, last_day, 0
        )
        df_all_launchs = pd.concat(([df_all_launchs, df_launchs]))
        unsuccesfull_launchs += nb_unsuccessful_launchs_of_year

        # Download data at 12Z
        df_launchs, nb_unsuccessful_launchs_of_year = get_data_for_year_and_hour_of_day(
            station_id, first_day, last_day, 12
        )
        df_all_launchs = pd.concat(([df_all_launchs, df_launchs]))
        unsuccesfull_launchs += nb_unsuccessful_launchs_of_year

    print(
        f"Done! There were {unsuccesfull_launchs} unsuccessful launchs in the specified period."
    )
    filename = (
        globals.AS_DATA_DIR
        + station_id
        + "_"
        + str(start_year)
        + "_"
        + str(end_year)
        + ".parquet.gzip"
    )
    print(
        f"Saving data on {df_all_launchs.shape[0]} observations to file {filename}.",
        end=" ",
    )
    df_all_launchs.to_parquet(filename, compression="gzip", index=False)
    print("Done!")


def main(argv):
    sounding_station_id = "SBGL"

    parser = argparse.ArgumentParser(
        description="""This script provides a simple interface for retrieving upper air
        sounding observations made by a user-provided station from the University of Wyoming Upper Air Archive.""",
        prog=sys.argv[0],
    )
    parser.add_argument(
        "-s", "--station_id", help="Atmospheric sounding station ID", default="SBGL"
    )
    parser.add_argument("-b", "--start_year", help="Start year", required=True)
    parser.add_argument("-e", "--end_year", help="End year", required=True)
    args = parser.parse_args()

    sounding_station_id = args.station_id
    try:
        start_year = int(args.start_year)
        end_year = int(args.end_year)
    except ValueError:
        print("Invalid date format. Use -h or --help for more information.")
        sys.exit(2)

    assert (sounding_station_id is not None) and (sounding_station_id != "")
    assert start_year <= end_year

    get_data(sounding_station_id, start_year, end_year)


if __name__ == "__main__":
    main(sys.argv)
