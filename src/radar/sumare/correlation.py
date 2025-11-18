import os
import math
import pandas as pd
import numpy as np
from PIL import Image
import argparse
from radar_data import RadarSumare
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
import csv
from pathlib import Path
from datetime import datetime
from typing import Tuple, Any
import warnings
warnings.filterwarnings("ignore")


class Weather_system:
    """
    Weather system handler used to load metadata and precipitation data
    from multiple meteorological systems (CEMADEN, INMET, ALERTARIO, etc).

    Attributes
    ----------
    system : str
        Selected weather system identifier.
    base_data_path : str
        Base directory where raw data files are stored.
    config_path : str
        Directory containing CSV configuration files.
    temp_resolution_system : dict
        Mapping between systems and their default temporal resolution.
    system_path : dict
        Mapping between systems and the relative data folder.
    """

    def __init__(self, system: str) -> None:
        self.system = system
        self.base_data_path = "./data/"
        self.config_path = "./config/"

        self.temp_resolution_system = {
            'cemaden': "1h",
            'inmet': "1h",
            'sumare': "2min",
            'alertario': "15min",
            'sirenes': "15min"
        }

        self.system_path = {
            'cemaden': "ws/cemaden/raw",
            'inmet': "ws/inmet",
            'sumare': "radar_sumare",
            'alertario': "ws/alerta-from-source",
            'sirenes': "web_sirenes_defesa_civil/data"
        }

    def get_station_info(self, station_name: str) -> Tuple[str, float, float]:
        """
        Retrieve station metadata (ID, latitude, longitude) for the given system.

        Parameters
        ----------
        station_name : str
            Human-readable station name as defined in the configuration file.

        Returns
        -------
        tuple
            (station_id, latitude, longitude)

        Raises
        ------
        ValueError
            If the station does not exist in the system's CSV file.
        """

        def get_id_lat_lon(df: pd.DataFrame, field: str, station_name: str) -> Tuple[str, float, float]:
            match = df.loc[df[field] == station_name]
            if match.empty:
                raise ValueError(
                    f"Estação '{station_name}' não encontrada no arquivo referente ao sistema {self.system}"
                )

            row = match.iloc[0]
            if self.system == "cemaden":
                return row["codestacao"], float(row["latitude"]), float(row["longitude"])
            return (
                row["STATION_ID"],
                float(row["VL_LATITUDE"]),
                float(row["VL_LONGITUDE"])
            )

        match self.system:
            case "cemaden":
                cemaden_info_stations = pd.read_csv(self.config_path + "cemaden_stations.csv")
                return get_id_lat_lon(cemaden_info_stations, "nome", station_name)

            case "inmet":
                inmet_info_stations = pd.read_csv(self.config_path + "SurfaceStation.csv")
                inmet_info_stations = inmet_info_stations[inmet_info_stations['SYSTEM'] == 'INMET']
                return get_id_lat_lon(inmet_info_stations, "DC_NOME", station_name)

            case "alertario":
                alertario_info_stations = pd.read_csv(self.config_path + "SurfaceStation.csv")
                alertario_info_stations = alertario_info_stations[alertario_info_stations['SYSTEM'] == 'ALERTARIO']
                return get_id_lat_lon(alertario_info_stations, "DC_NOME", station_name)

        raise ValueError(f"Unsupported system '{self.system}'")

    def get_data(self, id_station: str) -> pd.DataFrame:
        """
        Dispatch to the correct data loader based on weather system.

        Parameters
        ----------
        id_station : str
            Station ID used to locate data.

        Returns
        -------
        DataFrame
            Raw weather observations indexed by datetime.
        """
        match self.system:
            case 'cemaden':
                return self.cemaden_station_data(id_station)
            case 'inmet':
                return self.inmet_station_data(id_station)
            case 'alertario':
                return self.alertario_station_data(id_station)

        raise ValueError(f"Unsupported system '{self.system}'")

    def cemaden_station_data(self, station_code: str) -> pd.DataFrame:
        """
        Load CEMADEN station data from a parquet file.

        Parameters
        ----------
        station_code : str
            ID of the CEMADEN station.

        Returns
        -------
        DataFrame
            CEMADEN time series with precipitation values.
        """

        station_code = str(station_code)
        path = os.path.join(
            self.base_data_path,
            self.system_path.get(self.system),
            f"{station_code}.parquet"
        )

        print(f"Reading: {path}")
        cemaden_data_station = pd.read_parquet(path)
        cemaden_data_station["datahora"] = pd.to_datetime(
            cemaden_data_station["datahora"], errors="coerce", dayfirst=True
        )
        return cemaden_data_station

    def inmet_station_data(self, id_station: str) -> pd.DataFrame:
        """
        Placeholder for INMET data loader.
        """
        return pd.DataFrame()

    def alertario_station_data(self, id_station: str) -> pd.DataFrame:
        """
        Placeholder for ALERTARIO data loader.
        """
        return pd.DataFrame()


def compare_series(correlation_df: pd.DataFrame, system_label: str,
                   radar_label: str, station_name: str) -> None:
        """
        Plot two time series (precipitation vs reflectivity) on separate y-axes.

        Saves the figure as PNG.

        Parameters
        ----------
        correlation_df : DataFrame
            Merged system and radar dataframe.
        system_label : str
            Label for weather system.
        radar_label : str
            Label for radar data.
        station_name : str
            Station name for file naming.
        """
        fig, ax1 = plt.subplots(figsize=(10, 5))

        ax1.plot(correlation_df['datahora'], correlation_df['chuva'],
                 color='black', label=system_label, linewidth=2)
        ax1.set_xlabel("tempo")
        ax1.set_ylabel("Intensidade de Chuva")

        ax2 = ax1.twinx()
        ax2.plot(correlation_df['datahora'], correlation_df['reflect'],
                 color='tab:red', label=radar_label, linewidth=2, linestyle='--')
        ax2.set_ylabel("Refletividade")

        lines_1, labels_1 = ax1.get_legend_handles_labels()
        lines_2, labels_2 = ax2.get_legend_handles_labels()
        ax1.legend(lines_1 + lines_2, labels_1 + labels_2, loc="upper right")

        plt.title("Comportamento de chuva de refletividade com o tempo")
        plt.tight_layout()

        path = "./data/time_series/"
        os.makedirs(path, exist_ok=True)
        plt.savefig(path + f"{system_label}_{station_name}.png",
                    dpi=300, bbox_inches="tight")

        print(f"Time series Graph saved at {path}")


def correlation_graph(data: pd.DataFrame, x: str, y: str,
                      station_name: str, system: str) -> None:
    """
    Create and save a scatter plot of two numeric variables.

    Parameters
    ----------
    data : DataFrame
        Input dataset.
    x : str
        X-axis column name.
    y : str
        Y-axis column name.
    station_name : str
        Weather station name.
    system : str
        Weather system name.
    """
    path = "./data/correlation_graphs/"
    sns.scatterplot(x=x, y=y, data=data)
    plt.title(f"Correlação chuva-refletividade ( {system} - {station_name})")
    os.makedirs(path, exist_ok=True)
    plt.savefig(path + f"{system}_{station_name}.png",
                dpi=300, bbox_inches="tight")
    print(f"Correlation Graph saved at {path}")


def save_correlation_coefficients(system: str, station: str,
                                  spearman_stat: float, spearman_pvalue: float,
                                  pearson_stat: float, pearson_pvalue: float) -> None:
    """
    Append a line to a CSV file containing correlation metrics.

    Parameters
    ----------
    system : str
        Weather system.
    station : str
        Station name.
    spearman_stat : float
        Spearman correlation coefficient.
    spearman_pvalue : float
        Statistical p-value.
    pearson_stat : float
        Pearson correlation coefficient.
    pearson_pvalue : float
        Statistical p-value.
    """
    file_name = f"./data/correlacoes_{system}.csv"
    path = Path(file_name)

    header = [
        "data", "sistema", "estacao",
        "spearman_statistic", "spearman_pvalue",
        "pearson_statistic", "pearson_pvalue"
    ]

    new_line = [
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        system, station,
        spearman_stat, spearman_pvalue,
        pearson_stat, pearson_pvalue
    ]

    file_exists = path.exists()

    with path.open(mode='a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(header)
        writer.writerow(new_line)

    print(f"Linha salva em {path.resolve()}")


def parse_arguments() -> argparse.Namespace:
    """
    Parse CLI arguments for the correlation script.

    Returns
    -------
    argparse.Namespace
        Parsed arguments (`start`, `end`, `system`, `station`).
    """
    parser = argparse.ArgumentParser(
        description="Provides the correlation between radar and weather system data."
    )

    parser.add_argument("--start", required=True, type=str,
                        help="Start date (format: YYYY-MM-DD)")
    parser.add_argument("--end", required=True, type=str,
                        help="End date (format: YYYY-MM-DD)")

    parser.add_argument("--system", required=True, type=str, default="cemaden",
                        help="Weather system used for correlation")
    parser.add_argument("--station", required=True, type=str,
                        help="Station name in the selected system")

    return parser.parse_args()


def main(args: argparse.Namespace) -> None:
    """
    Main application workflow:
    - Load station metadata
    - Load weather system data
    - Load radar data
    - Merge and compute correlations
    - Generate plots and save metrics

    Parameters
    ----------
    args : argparse.Namespace
        Parsed CLI arguments.
    """
    print("--------------------------------------------------------------------------------------")
    weather_system = Weather_system(args.system)
    id_station, latitude, longitude = weather_system.get_station_info(args.station)
    print(f"weather system: {args.system}")
    print(f"station id: {id_station} lat: {latitude} lon: {longitude}")

    print(f"getting data from: {args.system}, station {args.station}...")
    system_data = weather_system.get_data(id_station)

    print(f"getting data from Radar Sumaré...")
    radar_system = RadarSumare("./data/radar_sumare")
    radar_data = radar_system.get_radar_data(
        args.start,
        args.end,
        weather_system.temp_resolution_system.get(args.system),
        latitude,
        longitude
    )

    # Merge system and radar data
    correlation_df = pd.merge(system_data, radar_data, on="datahora", how="inner")
    correlation_df = correlation_df.dropna(
        subset=["datahora", "reflect", "intensidade_precipitacao"]
    )

    correlation_graph(correlation_df, "chuva", "reflect", args.station, args.system)

    spearman_stat, spearman_pval = stats.spearmanr(
        correlation_df['chuva'], correlation_df['reflect']
    )
    pearson_stat, pearson_pval = stats.pearsonr(
        correlation_df['chuva'], correlation_df['reflect']
    )

    print(f"correlation coefficients: Spearman - {spearman_stat} | Pearson - {pearson_stat}")

    save_correlation_coefficients(
        args.system, args.station,
        spearman_stat, spearman_pval,
        pearson_stat, pearson_pval
    )

    compare_series(correlation_df, args.system, "Radar_Sumaré", args.station)

    print("End of processing.")
    print("--------------------------------------------------------------------------------------")


if __name__ == "__main__":
    args = parse_arguments()
    main(args)
