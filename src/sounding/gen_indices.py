#Esse código lê os perfis das radiossondas (arquivo Parquet) e calcula índices de instabilidade atmosférica (CAPE, CIN, Lifted Index, K-Index, etc.) com a biblioteca MetPy, salvando os resultados em outro arquivo Parquet.

import pandas as pd
from metpy.units import units                  # Adiciona unidades físicas (°C, hPa etc.)
import metpy.calc as mpcalc                    # Funções meteorológicas prontas (CAPE, CIN, índices de instabilidade)
import argparse                                # Para criar interface de linha de comando (CLI)

def compute_indices(df_launch):                # Função que calcula índices de instabilidade de UM lançamento
    # Remove linhas duplicadas pela coluna 'pressure'
    df_launch_cleaned = df_launch.drop_duplicates(subset='pressure', ignore_index=True)

    # Remove linhas com valores nulos em temperatura, ponto de orvalho ou pressão
    df_launch_cleaned = df_launch_cleaned.dropna(subset=('temperature', 'dewpoint', 'pressure'), how='any').reset_index(drop=True)

    # Ordena os dados por pressão (do nível mais alto de pressão → superfície, até menor pressão → altitudes maiores)
    df_launch_cleaned = df_launch_cleaned.sort_values('pressure', ascending=False)
        
    # Converte as colunas em listas e atribui as unidades corretas
    pressure_values = df_launch_cleaned['pressure'].to_list() * units.hPa
    temperature_values = df_launch_cleaned['temperature'].to_list() * units.degC
    dewpoint_values = df_launch_cleaned['dewpoint'].to_list() * units.degC

    # Calcula o perfil da parcela de ar que sobe na atmosfera (parcel profile)
    parcel_profile = mpcalc.parcel_profile(pressure_values, 
                                           temperature_values[0],  # temp da superfície
                                           dewpoint_values[0]      # ponto de orvalho da superfície
                                           ).to('degC')

    indices = dict()  # Dicionário para armazenar os índices calculados

    # CAPE (energia de convecção) e CIN (energia de inibição)
    CAPE = mpcalc.cape_cin(pressure_values, temperature_values, dewpoint_values, parcel_profile)
    indices['cape'] = CAPE[0].magnitude       # CAPE em J/kg
    indices['cin'] = CAPE[1].magnitude        # CIN em J/kg

    # Lifted Index (LI): diferença de temp em 500 hPa (instabilidade)
    lift = mpcalc.lifted_index(pressure_values, temperature_values, parcel_profile)
    indices['lift'] = lift[0].magnitude

    # K-Index: usado para prever tempestades
    k = mpcalc.k_index(pressure_values, temperature_values, dewpoint_values)
    indices['k'] = k.magnitude

    # Total Totals Index: combina temp e ponto de orvalho → instabilidade
    total_totals = mpcalc.total_totals_index(pressure_values, temperature_values, dewpoint_values)
    indices['total_totals'] = total_totals.magnitude

    # Showalter Index: outro índice clássico de instabilidade (comparação em 500 hPa)
    showalter = mpcalc.showalter_index(pressure_values, temperature_values, dewpoint_values)
    indices['showalter'] = showalter.magnitude[0]

    return indices  # Retorna todos os índices em formato de dicionário

def main():
    # Define argumentos de linha de comando
    parser = argparse.ArgumentParser(description='Generate instability indices from sounding measurements.')
    parser.add_argument('--input_file', help='Parquet file name containing the sounding measurements', required=True)
    parser.add_argument('--output_file', help='Parquet file name where the indices are going to be saved', required=True)
    args = parser.parse_args()

    # Lê o arquivo Parquet com todos os lançamentos da radiossonda
    df_s = pd.read_parquet(args.input_file)

    # Cria DataFrame vazio com as colunas dos índices
    df_indices = pd.DataFrame(columns=['time', 'cape', 'cin', 'lift', 'k', 'total_totals', 'showalter'])

    # Loop em cada lançamento único (cada horário/time diferente)
    for launch_timestamp in pd.to_datetime(df_s.time).unique():
        print(f"Generating instability indices for launch made at {launch_timestamp}...", end="")
        try:
            # Filtra só os dados desse lançamento
            df_launch = df_s[pd.to_datetime(df_s['time']) == launch_timestamp]
            # Calcula os índices com a função compute_indices
            indices_dict = compute_indices(df_launch)
            # Adiciona a data/hora do lançamento ao dicionário
            indices_dict['time'] = launch_timestamp
            # Concatena no DataFrame de saída
            df_indices = pd.concat([df_indices, pd.DataFrame.from_records([indices_dict])])
            print("Success!")  # Log se deu certo
        except ValueError as e:  # Erros de valor (dados ruins)
            print(f'Error processing measurements made by launch at {launch_timestamp}')
            print(f'{repr(e)}')
        except IndexError as e:  # Erros de índice (dados faltando)
            print(f'Error processing measurements made by launch at {launch_timestamp}')
            print(f'{repr(e)}')
        except KeyError as e:    # Erros de chave (colunas faltando)
            print(f'Error processing measurements made by launch at {launch_timestamp}')
            print(f'{repr(e)}')

    # Salva os índices em um novo arquivo Parquet comprimido
    df_indices.to_parquet(args.output_file, compression='gzip', index=False)

    print("Done!")  # Finaliza o programa

if __name__ == "__main__":
    main()  # Ponto de entrada do programa: só roda se for executado pelo terminal
