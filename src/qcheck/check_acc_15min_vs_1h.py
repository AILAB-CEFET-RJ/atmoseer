import argparse
from datetime import timedelta
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from tqdm import tqdm


class Websirene1h15mQualityCheck:
    def _check_websirenes_keys_exists(self, websirene_key: str, fn_disabled=True) -> str:
        if fn_disabled:
            return websirene_key

        try:
            current_dir = Path.cwd()
            websirenes_keys_dir = current_dir / "websirenes_keys"
            if not websirenes_keys_dir.exists():
                raise FileNotFoundError("websirenes_keys folder not found in the current directory")

            parquet_files = list(websirenes_keys_dir.glob("*.parquet"))
            if not parquet_files or not len(parquet_files) > 0:
                raise FileNotFoundError("No .parquet files found in websirenes_keys folder")

            websirene_key_path = websirenes_keys_dir / websirene_key
            if not websirene_key_path.exists():
                raise FileNotFoundError(f"{websirene_key} not found in websirenes_keys folder")

            filename = websirene_key_path.stem
            return filename
        except FileNotFoundError as e:
            print(f"Error: {e}")
            exit(1)

    def _show_info(self, df: pd.DataFrame):
        print("head:")
        print(df.head())

        print("describe:")
        print(df.describe())

        print("min index:")
        print(df.index.min())

        print("max index:")
        print(df.index.max())

    def compare_15min_1h(self, parquet: str):
        filename = self._check_websirenes_keys_exists(parquet)

        df = pd.read_parquet(parquet)
        self._show_info(df)

        minimum_date = df.index.min().replace(minute=0, second=0, microsecond=0)
        minimum_date = (
            minimum_date if minimum_date >= df.index.min() else minimum_date + timedelta(hours=1)
        )

        print(f"Using minimum date: {minimum_date}")

        maximum_date = df.index.max().replace(minute=0, second=0, microsecond=0)
        maximum_date = (
            maximum_date if maximum_date <= df.index.max() else maximum_date - timedelta(hours=1)
        )
        print(f"Using maximum date: {maximum_date}")

        hourly_range = pd.date_range(start=minimum_date, end=maximum_date, freq="H")

        total_processed = 0

        diff_series = []

        for hour in tqdm(hourly_range):
            time_upper_bound = hour
            time_lower_bound = hour - timedelta(minutes=45)

            df_filtered = df[(df.index >= time_lower_bound) & (df.index <= time_upper_bound)]

            h01 = df[df.index == time_upper_bound]["h01"]
            m15 = df_filtered["m15"]

            if not np.isclose(m15.sum(), h01.sum(), rtol=1e-05):
                m15_value = m15.sum()
                h1_value = h01.sum()
                diff_series.append(np.abs(m15_value - h1_value))
            total_processed += 1

        fig, ax = plt.subplots(figsize=(12, 6))

        ax.boxplot(diff_series, labels=["Absolute Difference"])
        plt.title(
            f"Absolute Difference between h01 and m15 {len(diff_series)}/{total_processed} are different"
        )
        plt.ylabel("Diff Value")
        output_file = Path.cwd() / f"{filename}_boxplot_1h_15minAggregated.png"
        green = "\033[92m"
        reset = "\033[0m"
        print(f"{green}Saving boxplot to {output_file}{reset}")
        plt.savefig(output_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Check if the 15min sum is equal to the 1h sum.\n\n"
            "Usage example:\n"
            'python check_acc_15min_vs_1h.py --station-type websirenes --parquet="-22.8652_-43.2805.parquet"'
        ),
        formatter_class=argparse.RawTextHelpFormatter,  # Ensures proper formatting for multiline help text
    )
    parser.add_argument(
        "--station-type",
        dest="station_type",
        type=str,
        required=True,
        help="Station name",
        choices=["websirenes", "inmet", "alertario"],
    )
    parser.add_argument(
        "--parquet",
        type=str,
        required=True,
        help='Name of the parquet file, for example: "-22.8652_-43.2805.parquet"',
    )

    args = parser.parse_args()

    if args.station_type == "websirenes":
        station_check = Websirene1h15mQualityCheck()
    # TODO: alertario and inmet

    station_check.compare_15min_1h(args.parquet)
