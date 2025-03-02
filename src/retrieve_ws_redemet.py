import datetime as dt
import pandas as pd
import requests
import json
import csv
import argparse
import time

def qtd_stations():
    stations_rj = ['SBGL', 'SBAF', 'SBRJ', 'SBJR', 'SBSC']
    print("Na cidade do Rio de Janeiro temos o total de:", len(stations_rj), "estações meteorológicas", (stations_rj))
    print("Iniciando")

def start_data(api_key, station, start_date, end_date, output_file, log_file):
    current_date = start_date
    dataframes = []
    log_data = []
    error_log = []
    start_time = time.time()
    contador = 0

    while current_date <= end_date:
        try:
            url = requests.get(f"https://api-redemet.decea.mil.br/aerodromos/info?api_key={api_key}&localidade={station}&datahora={current_date}")
            url.raise_for_status()
            clima = json.loads(url.text)

            if clima:
                df = pd.DataFrame(clima)

                if 'message' in clima:
                    df = df.drop("message", axis=1)

                if 'status' in clima:
                    df = df.drop("status", axis=1)

                dataset = df.T

                dataset.rename(columns={'ur': 'relative_humidity'}, inplace=True)
                dataset.rename(columns={'data': 'datatime'}, inplace=True)
                dataset['barometric_pressure'] = None
                dataset['wind_speed'] = None
                dataset['wind_dir'] = None

                if 'barometric_pressure' in dataset:
                    dataset['barometric_pressure'] = dataset['metar'].str.extract(r'(Q\d{4})')
                    dataset['barometric_pressure'] = dataset['barometric_pressure'].str.replace('Q', '', regex=True)
                   

                if 'vento' in dataset:
                    dataset['wind_speed'] = dataset['vento'].str.extract(r'(\d{1,2}km/h)')
                    dataset['wind_dir'] = dataset['vento'].str.extract(r'(\d{2,3}º)')
                    dataset['wind_dir'] = dataset['wind_dir'].str.replace('º', '')
                    

                if 'nome' in dataset:
                    dataset = dataset.drop("nome", axis=1)

                if 'ceu' in dataset:
                    dataset = dataset.drop("ceu", axis=1)

                if 'cidade' in dataset:
                    dataset = dataset.drop("cidade", axis=1)

                if 'condicoes_tempo' in dataset:
                    dataset = dataset.drop("condicoes_tempo", axis=1)

                if 'localizacao' in dataset:
                    dataset = dataset.drop("localizacao", axis=1)

                if 'metar' in dataset:
                    dataset = dataset.drop("metar", axis=1)

                if 'tempoImagem' in dataset:
                    dataset = dataset.drop("tempoImagem", axis=1)

                if 'teto' in dataset:
                    dataset = dataset.drop("teto", axis=1)

                if 'visibilidade' in dataset:
                    dataset = dataset.drop("visibilidade", axis=1)

                if 'vento' in dataset:
                    dataset = dataset.drop("vento", axis=1)

                if 'lat' in dataset:
                    dataset = dataset.drop("lat", axis=1)

                if 'lon' in dataset:
                    dataset = dataset.drop("lon", axis=1)

                dataframes.append(dataset)
                log_entry = {
                    "Data": current_date,
                    "Status": "Sucesso",
                }
                log_data.append(log_entry)
            else:
                log_entry = {
                    "Data": current_date,
                    "Status": "Erro: Sem dados",
                }

                
                log_data.append(log_entry)
            contador += 1
        except requests.exceptions.RequestException as e:

            if "429" and "443" in str(e):

                print(f"Erro 429: Muitas solicitações. Primeiro looping")
                log_entry = {
                    "Data": current_date,
                    "Status": f"Erro 429: Muitas solicitações",
                }
                error_log.append(log_entry)
                retry_data.append(current_date)  
                time.sleep(1800)
            else:
                print(f"Erro na solicitação HTTP: {e}")
                break
        current_date = (dt.datetime.strptime(current_date, "%Y%m%d%H") + dt.timedelta(hours=1)).strftime("%Y%m%d%H")

    end_time = time.time()
    total_time = end_time - start_time
    log_df = pd.DataFrame(log_data)
    log_df.to_csv(log_file, index=False)


    if dataframes:
        combined_df = pd.concat(dataframes, ignore_index=True)
        combined_df.to_csv(f"{output_file}.csv", index=False)
        print(f'Dados salvos em {output_file}.csv')
    print(f'Tempo total: {total_time} segundos')
    print(f'O máximo de requisições é de {contador}, após isso precisa fazer uma nova com uma chave nova')

def argumentos():
    description = "Escolha a estação meteorológica, sua chave de API e as datas de início e fim no formato AAAAMMDDHH."
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("-k", "--key", required=True, help="Chave de API")
    parser.add_argument("-s", "--station", required=True, help="Estação meteorológica (exemplo: SBBR)")
    parser.add_argument("-start", "--start_date", required=True, help="Data de início no formato AAAAMMDDHH")
    parser.add_argument("-end", "--end_date", required=True, help="Data de fim no formato AAAAMMDDHH")
    parser.add_argument("-o", "--output_file", required=True, help="Nome do arquivo de saída CSV")
    return parser.parse_args()

if __name__ == "__main__":
    qtd_stations()
    args = argumentos()
    log_file = "log.csv"
    start_data(args.key, args.station, args.start_date, args.end_date, args.output_file, log_file)
