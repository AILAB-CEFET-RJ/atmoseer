import argparse
from typing import Optional

import pandas as pd

from .Logger import logger
from .WebSirenesCoords import websirenes_coords_path
from .WebSirenesKeys import websirenes_keys
from .WebSirenesParser import WebSirenesParser
from .WebsirenesTarget import websirenes_target

log = logger.get_logger(__name__)


def validate_dates(
    start_date: Optional[str], end_date: Optional[str]
) -> tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
    if not start_date or not end_date:
        return None, None

    s_date = pd.Timestamp(start_date)
    e_date = pd.Timestamp(end_date)

    MIN_START_DATE = pd.Timestamp("2011-04-12 20:30:00")
    MAX_END_DATE = pd.Timestamp("2022-06-02 21:30:00")

    if not (MIN_START_DATE <= s_date <= e_date <= MAX_END_DATE):
        raise ValueError(
            f"start_date and end_date must be between {MIN_START_DATE} and {MAX_END_DATE}"
        )

    log.info(f"Building spatiotemporal data from {s_date} to {e_date}")
    return s_date, e_date


def check_data_requirements():
    try:
        websirenes_defesa_civil_path = WebSirenesParser.websirenes_defesa_civil_path
        if not websirenes_defesa_civil_path.exists():
            raise FileNotFoundError(
                f"websirenes_defesa_civil folder not found in {websirenes_defesa_civil_path}. Please place the websirenes dataset in the expected folder"
            )
        if not any(websirenes_defesa_civil_path.glob("*.txt")):
            raise FileNotFoundError(f"No txt files found in {websirenes_defesa_civil_path}")

        if not websirenes_coords_path.exists():
            raise FileNotFoundError(
                f"websirenes_coords.parquet not found in {websirenes_coords_path}. Please place the websirenes coordinates dataset in the expected folder"
            )

        if not websirenes_target.era5land_path.exists():
            raise FileNotFoundError(
                f"ERA5Land folder not found in {websirenes_target.era5land_path}. Please place the ERA5Land dataset in the expected folder"
            )

        if not (websirenes_target.era5land_path / "monthly_data").exists():
            raise FileNotFoundError(
                f"ERA5Land monthly_data folder not found in {websirenes_target.era5land_path}. Please place the ERA5Land monthly data in the expected folder"
            )

        if not any((websirenes_target.era5land_path / "monthly_data").glob("*.nc")):
            raise FileNotFoundError(
                f"No nc files found in {websirenes_target.era5land_path / 'monthly_data'}. Please place the ERA5Land monthly data in the expected folder"
            )

    except Exception as e:
        log.error(f"Error while checking data requirements: {e}")
        exit(1)


def build_target(start_date: Optional[pd.Timestamp], end_date: Optional[pd.Timestamp]):
    try:
        websirenes_keys.build_keys(start_date, end_date)
        websirenes_target.build_timestamps_hourly(start_date, end_date)
    except Exception as e:
        log.error(f"Error while building target: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build spatiotemporal data")
    parser.add_argument(
        "--start_date",
        type=str,
        required=False,
        help="Start date in the format YYYY-MM-DDTHH:MM:SS",
    )
    parser.add_argument(
        "--end_date",
        type=str,
        required=False,
        help="End date in the format YYYY-MM-DDTHH:MM:SS",
    )

    check_data_requirements()

    args = parser.parse_args()

    start_date, end_date = validate_dates(args.start_date, args.end_date)

    build_target(start_date, end_date)
