#Esse script baixa automaticamente dados de radiossondas (00Z e 12Z) da Universidade de Wyoming para um período e uma estação escolhidos, e salva tudo em um arquivo único no formato Parquet.


# -*- coding: utf-8 -*-                         # Declara a codificação do arquivo (útil p/ acentos).
"""
This script provides a simple interface for retrieving upper air sounding 
observations from the University of Wyoming Upper Air Archive. 

It uses the WyomingUpperAir class in the siphon library. In particular, 
the request_data method of the WyomingUpperAir class takes two arguments: 
 - the date of the sounding launch as a datetime object, 
 - the atmospheric sounding station ID (as a string).

For an overview of weather balloons, see:
https://www.weather.gov/media/key/Weather-Balloons.pdf
"""                                             # Docstring: descreve o propósito do script e a fonte dos dados.

import pandas as pd                             # Importa pandas para manipular DataFrames e salvar Parquet.
import sys                                      # Acessa argv e permite sair com sys.exit.
import argparse                                 # Faz o parsing dos argumentos de linha de comando (CLI).
from datetime import datetime, timedelta        # Trabalha com datas e somas de tempo (horas, dias).
from siphon.simplewebservice.wyoming import WyomingUpperAir  # Cliente Siphon para baixar as sondagens.
import time                                     # Fornece sleep para pausar entre tentativas.
import requests                                 # Para capturar exceções HTTP (usado dentro do Siphon).
from src.config import globals                  # Configurações do projeto (ex.: AS_DATA_DIR).

def get_data_for_period(station_id, first_day, last_day):  # Função que coleta dados 00Z e 12Z entre duas datas.
            #00Z → 00:00 UTC
            #12Z → 12:00 UTC
    unsuccesfull_launchs = 0                    # Contador de lançamentos sem sucesso (sem dados ou erro).
    next_day = first_day                        # Cursor do dia atual a ser processado.
    df_all_launchs = pd.DataFrame()             # Acumulador de todas as sondagens do período (DataFrame).

    while next_day <= last_day:                 # Loop dia a dia do intervalo [first_day, last_day].
        for hour_of_day in [0, 12]:             # Para cada dia, tenta nos horários padrão: 00Z e 12Z. 

            current_launch_time = next_day + timedelta(hours=hour_of_day)  # Timestamp da sondagem desejada.

            try:                                # Bloco de tentativa para baixar a sondagem desse timestamp.
                print(f"Downloading {current_launch_time}...", end="")     # Log sem quebrar linha.
                df_launch = WyomingUpperAir.request_data(current_launch_time, station_id)  
                                                # Chama a API do Wyoming via Siphon; retorna DataFrame da sondagem.
                print(f" OK ({len(df_launch)} obs).")   # Loga sucesso + número de níveis/linhas retornados.
                df_all_launchs = pd.concat([df_all_launchs, df_launch])    # Concatena no acumulador (custo alto se muitas iterações).
            except IndexError as e:             # Sem dados para esse horário (comum em alguns dias/estações).
                print(f"IndexError: {repr(e)}") # Loga o erro específico.
                unsuccesfull_launchs += 1       # Incrementa contador de falhas.
            except ValueError as e:             # Erros de valor/formato vindos do provedor/biblioteca.
                print(f"ValueError: {str(e)}")  # Loga mensagem do erro.
                unsuccesfull_launchs += 1       # Conta como falha.
            except requests.HTTPError as e:     # Falha HTTP (servidor ocupado/instável, timeouts etc.).
                print(f"HTTPError: {repr(e)}")  # Loga a exceção HTTP.
                print(f"Server busy at {current_launch_time}, retrying in 10s...")  
                                                # Informa que vai tentar novamente.
                time.sleep(10)                  # Dorme 10 segundos antes de prosseguir.
                continue                        # Volta para o próximo passo do for (não incrementa o dia aqui).
            except Exception as e:              # Qualquer outra exceção não prevista.
                print(f"Unexpected error: {repr(e)}")    # Loga erro inesperado.
                sys.exit(2)                     # Encerra o programa com código de saída 2.

        next_day += timedelta(days=1)           # Após tentar 00Z e 12Z, avança para o dia seguinte.

    return df_all_launchs, unsuccesfull_launchs # Retorna o DataFrame acumulado e o total de falhas no período.

def get_data(station_id, start_date, end_date): # Função “orquestradora”: baixa e salva do período informado.
    print(f"Downloading observations from {station_id} between {start_date.date()} and {end_date.date()}...")
                                                # Log inicial informando estação e intervalo de datas.
    df_all_launchs, unsuccesfull_launchs = get_data_for_period(station_id, start_date, end_date)
                                                # Chama a função que percorre dia a dia e horários 00/12Z.

    print(f"Done! {unsuccesfull_launchs} failed launches.")  # Log final de falhas (sem dados/erros).
    filename = globals.AS_DATA_DIR + f"{station_id}_{start_date.date()}_{end_date.date()}.parquet.gzip"
                                                # Monta o caminho do arquivo de saída (usa diretório de config).
    df_all_launchs.to_parquet(filename, compression='gzip', index=False)   
                                                # Salva o DataFrame completo em Parquet comprimido (precisa pyarrow/fastparquet).
    print(f"Saved {df_all_launchs.shape[0]} observations to {filename}")  
                                                # Loga quantas linhas (níveis de sondagem) foram salvas.

def main(argv):                                 # Função principal para rodar como CLI.
    parser = argparse.ArgumentParser(           # Cria o parser de argumentos do terminal.
        description="Retrieve upper air sounding observations from University of Wyoming Upper Air Archive.",
        prog=sys.argv[0]                        # Nome do programa (usado em mensagens de help).
    )
    parser.add_argument('-s', '--station_id', help='Atmospheric sounding station ID', default='SBGL')
                                                # Estação upper-air (default SBGL = Galeão/Rio).
    parser.add_argument('-b', '--start_year', help='Start year', required=False)
                                                # Alternativa 1: informar anos (início).
    parser.add_argument('-e', '--end_year', help='End year', required=False)
                                                # Alternativa 1: informar anos (fim).
    parser.add_argument('--start_date', help='Start date (YYYY-MM-DD)', required=False)
                                                # Alternativa 2: informar datas completas (início).
    parser.add_argument('--end_date', help='End date (YYYY-MM-DD)', required=False)
                                                # Alternativa 2: informar datas completas (fim).
    args = parser.parse_args()                  # Faz o parsing real dos argumentos passados no terminal.

    # Decide se usa ano inteiro ou intervalo de datas
    if args.start_date and args.end_date:       # Se o usuário passou datas (string no formato YYYY-MM-DD)...
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d")  # Converte início p/ datetime.
        end_date   = datetime.strptime(args.end_date, "%Y-%m-%d")    # Converte fim p/ datetime.
    elif args.start_year and args.end_year:     # Caso contrário, se passou anos...
        start_date = datetime(int(args.start_year), 1, 1)            # Usa 1º de jan do ano inicial.
        end_date   = datetime(int(args.end_year), 12, 31)            # Usa 31 de dez do ano final.
    else:                                       # Se não passou nenhuma combinação válida...
        print("Error: provide either --start_date and --end_date OR --start_year and --end_year")
                                                # Mensagem de uso incorreto.
        sys.exit(1)                             # Sai com erro.

    get_data(args.station_id, start_date, end_date)  # Dispara a coleta + salvamento com os parâmetros decididos.

if __name__ == "__main__":                      # Garante execução deste bloco só quando o arquivo é rodado direto.
    main(sys.argv)                              # Chama a função principal, repassando argumentos da linha de comando.
