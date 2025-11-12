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
import warnings
warnings.filterwarnings("ignore")

class Weather_system:
    def __init__(self, system):

        self.system = system
        self.base_data_path = "./data/"
        self.config_path = "./config/"

        self.temp_resolution_system = { 'cemaden': "1h",
                                        'inmet' : "1h",
                                        'sumare' : "2min",
                                        'alertario' : "15min",
                                        'sirenes' : "15min"
                                        }
        
        self.system_path = {'cemaden': "ws/cemaden/raw",
                            'inmet' : "ws/inmet",
                            'sumare' : "radar_sumare",
                            'alertario' : "ws/alerta-from-source",
                            'sirenes' : "web_sirenes_defesa_civil/data"
                            }
 
    def get_station_info(self, station_name):
        if self.system == 'cemaden':
            cemaden_info_stations = pd.read_csv(self.config_path + "cemaden_stations.csv")
            match = cemaden_info_stations.loc[cemaden_info_stations["nome"] == station_name]

            if match.empty:
                raise ValueError(f"Estação '{station_name}' não encontrada no arquivo cemaden_stations.csv")

            row = match.iloc[0]
            return row["codestacao"], row["latitude"], row["longitude"]


                    
    def get_data(self, id_station):
        if self.system == 'cemaden':
            return self.cemaden_station_data(id_station)
        

    
    def cemaden_station_data(self, station_code):
        station_code = str(station_code)  # garante que é string
        path = os.path.join(self.base_data_path, self.system_path.get(self.system), f"{station_code}.parquet")
        print(f"Reading: {path}")

        cemaden_data_station = pd.read_parquet(path)
        cemaden_data_station["datahora"] = pd.to_datetime(
            cemaden_data_station["datahora"], errors="coerce", dayfirst=True
        )
        return cemaden_data_station


def compare_series(correlation_df,system_label, radar_label, station_name):

    fig, ax1 = plt.subplots(figsize=(10, 5))

    # Primeira série - eixo Y à esquerda
    ax1.plot(correlation_df['datahora'], correlation_df['chuva'], color='black', label=system_label, linewidth=2)
    ax1.set_xlabel("tempo")
    ax1.set_ylabel("Intensidade de Chuva")
    ax1.tick_params(axis='y')

    # Segunda série - eixo Y à direita
    ax2 = ax1.twinx()
    ax2.plot(correlation_df['datahora'], correlation_df['reflect'], color='tab:red',label=radar_label, linewidth=2, linestyle='--')
    ax2.set_ylabel("Refletividade")
    ax2.tick_params(axis='y')

    # Combinar legendas
    lines_1, labels_1 = ax1.get_legend_handles_labels()
    lines_2, labels_2 = ax2.get_legend_handles_labels()
    ax1.legend(lines_1 + lines_2, labels_1 + labels_2, loc="upper right")

    # Título e layout
    plt.title("Comportamento de chuva de refletividade com o tempo")
    plt.tight_layout()

    path = "./data/time_series/"
    os.makedirs(path, exist_ok=True)
    plt.savefig(path + f"{system_label}_{station_name}.png" , dpi=300, bbox_inches="tight")   # PNG em alta resolução
    print(f"Time series Graph saved at {path}")

def correlation_graph(data,x,y,station_name,system):# Gráfico de dispersão com seaborn
    path = "./data/correlation_graphs/"
    sns.scatterplot(x=x, y=y, data=data)
    plt.title(f"Correlação chuva-refletividade ( {system} - {station_name})")
    os.makedirs(path, exist_ok=True)
    plt.savefig(path + f"{system}_{station_name}.png" , dpi=300, bbox_inches="tight")   # PNG em alta resolução
    print(f"Correlation Graph saved at {path}")

def save_correlation_coefficients(system: str, station: str, spearman_stat: float, spearman_pvalue: float, pearson_stat: float, pearson_pvalue: float):

    file_name = f"./data/correlacoes_{system}.csv"
    path = Path(file_name)


    header = ["data", "sistema", "estacao", "spearman_statistic", "spearman_pvalue", "pearson_statistic", "pearson_pvalue"]
    new_line = [datetime.now().strftime("%Y-%m-%d %H:%M:%S"), system, station, spearman_stat, spearman_pvalue, pearson_stat, pearson_pvalue]


    file_exists = path.exists()

    with path.open(mode='a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(header)
        writer.writerow(new_line)

    print(f"Linha salva em {path.resolve()}")

def parse_arguments():
    parser = argparse.ArgumentParser(description="Provides de Correlation relationship between radar data and a provided weather system")


    # Required arguments
    parser.add_argument("--start", required=True, type=str,
                        help="Start date (format: YYYY-MM-DD)")
    parser.add_argument("--end", required=True, type=str,
                        help="End date (format: YYYY-MM-DD)")

    # Optional arguments
    parser.add_argument("--system", required=True, type=str, default="cemaden",
                        help="weather system chosen to correlation with radar data")
    parser.add_argument("--station", required=True, type=str,
                        help="station name in the chosen weather system")

    return parser.parse_args()

def main(args):
    print("--------------------------------------------------------------------------------------")
    weather_system = Weather_system(args.system)
    id_station, latitude, longitude = weather_system.get_station_info(args.station)
    print(f"weather system: {args.system}")
    print(f"station id: {id_station} lat: {latitude} lon: {longitude}")

    print(f"getting data from: {args.system}, station {args.station}...")
    system_data = weather_system.get_data(id_station)

    print(f"getting data from Radar Sumaré...")
    radar_system = RadarSumare("./data/radar_sumare")
    radar_data = radar_system.get_radar_data(args.start, 
                                            args.end, 
                                            weather_system.temp_resolution_system.get(args.system),
                                            latitude, 
                                            longitude)
    
    correlation_df = pd.merge(system_data, radar_data, on="datahora", how="inner")
    correlation_df = correlation_df.dropna(subset=["datahora", "reflect", "intensidade_precipitacao"])
    correlation_graph(correlation_df,"chuva", "reflect",args.station, args.system)

    spearman_stat, spearman_pval = stats.spearmanr(correlation_df['chuva'],correlation_df['reflect'])
    pearson_stat, pearson_pval =  stats.pearsonr(correlation_df['chuva'],correlation_df['reflect'])
    print('correlation coefficients: Spearman -' + str(spearman_stat) + " Pearson - " + str(pearson_stat))
    save_correlation_coefficients(args.system, args.station, spearman_stat, spearman_pval, pearson_stat, pearson_pval)

    compare_series(correlation_df,args.system, "Radar_Sumaré", args.station)

    print("End of processing.")
    print("--------------------------------------------------------------------------------------")


if __name__ == "__main__":
    args = parse_arguments()
    main(args)

