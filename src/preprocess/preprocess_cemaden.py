import pandas as pd
import numpy as np
from pathlib import Path
import argparse
import logging
import os
from sklearn.impute import KNNImputer
from datetime import datetime
from metpy.calc import wind_components
from metpy.units import units

from config import globals

def add_hour_related_features(df):
    dt = df.index
    hourfloat = dt.hour + dt.minute / 60.0
    df['hour_sin'] = np.sin(2. * np.pi * hourfloat / 24.)
    df['hour_cos'] = np.cos(2. * np.pi * hourfloat / 24.)
    return df


def transform_wind(wind_speed, wind_direction, comp_idx):
    assert comp_idx in [0, 1]
    return wind_components(wind_speed * units('m/s'), wind_direction * units.deg)[comp_idx].magnitude


def add_wind_related_features(df):
    df['wind_direction_u'] = df.apply(lambda x: transform_wind(x.wind_speed, x.wind_dir, 0), axis=1)
    df['wind_direction_v'] = df.apply(lambda x: transform_wind(x.wind_speed, x.wind_dir, 1), axis=1)
    return df


def min_max_normalize(df):
    return (df - df.min()) / (df.max() - df.min())


def preprocess(filename, output_folder):
    logging.info(f"Loading datasource file {filename}.")
    df = pd.read_parquet(filename)
    logging.info(df.head())

    # 1. Converter para datetime (sem UTC ainda)
    df['datahora'] = pd.to_datetime(df['datahora'])

    # 2. Arredondar para hora cheia
    df['hora_cheia'] = df['datahora'].dt.floor('H')

    # 3. Agregar por hora cheia: pegar máximo de 'chuva' e 'intensidade_precipitacao'
    colunas_para_agregar = []
    if 'intensidade_precipitacao' in df.columns:
        colunas_para_agregar.append('intensidade_precipitacao')
    if 'chuva' in df.columns:
        colunas_para_agregar.append('chuva')

    if colunas_para_agregar:
        max_por_hora = df.groupby('hora_cheia')[colunas_para_agregar].max().reset_index()
        if 'intensidade_precipitacao' in max_por_hora.columns:
            max_por_hora = max_por_hora.rename(columns={'intensidade_precipitacao': 'intensidade_max'})
        if 'chuva' in max_por_hora.columns:
            max_por_hora = max_por_hora.rename(columns={'chuva': 'chuva_max'})

        df = df.merge(max_por_hora, on='hora_cheia', how='left')

        if 'intensidade_max' in df.columns:
            df['intensidade_precipitacao'] = df['intensidade_max']
            df = df.drop(columns=['intensidade_max'])
        if 'chuva_max' in df.columns:
            df['chuva'] = df['chuva_max']
            df = df.drop(columns=['chuva_max'])

    # 4. Substituir datahora pela hora cheia com UTC
    df['datahora'] = df['hora_cheia'].dt.tz_localize('UTC')
    df = df.drop(columns=['hora_cheia'])

    df = df.set_index(pd.DatetimeIndex(df['datahora']))
    logging.info("Datetime index added.")

    # Define mapeamento de colunas baseado em sensores típicos do CEMADEN tipo 1
    column_name_mapping = {
        "datahora": "datetime",
        "chuva": "precipitation",
        "intensidade da Precipitação": "precipitation_intensity",
        "temperatura do Ar": "temperature",
        "umidade Relativa do Ar": "relative_humidity",
        "velocidade do Vento": "wind_speed",
        "direção do Vento": "wind_dir",
    }

    df = df.rename(columns=column_name_mapping)
    logging.info("Column names standardized.")

    # Definir variáveis preditoras e alvo
    predictors = ['temperature', 'relative_humidity', 'wind_speed', 'wind_dir', 'hour_sin', 'hour_cos']
    target = 'precipitation'

    # Filtrar apenas colunas relevantes disponíveis
    available_predictors = [c for c in predictors if c in df.columns]
    if target not in df.columns:
        logging.warning("Target variable 'precipitation' not found. Skipping file.")
        return

    df = df[available_predictors + [target]].copy()
    df = df[df[target].notna()]
    logging.info(f"Remaining observations after dropping NaN in target: {len(df)}")

    if 'wind_speed' in df.columns and 'wind_dir' in df.columns:
        df = add_wind_related_features(df)

    df = add_hour_related_features(df)
    df = df.drop(columns=['wind_speed', 'wind_dir'], errors='ignore')

    predictors_final = [col for col in df.columns if col != target]
    target_column = df[target]
    df = df[predictors_final]
    df = min_max_normalize(df)

    logging.info("Applying KNN imputation...")
    imputer = KNNImputer(n_neighbors=2)
    df[:] = imputer.fit_transform(df)

    df[target] = target_column
    filename_new = Path(filename).stem + '_preprocessed.parquet.gzip'
    output_path = Path(output_folder) / filename_new
    print(df)
    df.to_parquet(output_path, compression='gzip')
    logging.info(f"Saved preprocessed file to {output_path}")


def main():
    parser = argparse.ArgumentParser(description='Preprocess CEMADEN weather station data.')
    parser.add_argument('-f', '--file', required=True, help='Path to the raw .parquet file')
    args = parser.parse_args()
    file = globals.CEMADEN_DATA_DIR + "raw/" + args.file + ".parquet"
    output = globals.CEMADEN_DATA_DIR + "preprocessed/"
    logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
    preprocess(file, output)


if __name__ == '__main__':
    main()
