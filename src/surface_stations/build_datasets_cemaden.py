import os
import sys
import yaml
import pickle
import logging
import argparse
import numpy as np
import pandas as pd
import datetime
from typing import List

from config import globals

def split_dataframe_by_date(df, threshold_date):
    threshold_date = pd.to_datetime(threshold_date).tz_localize('UTC')
    df_train_val = df[df.index < threshold_date]
    df_test = df[df.index >= threshold_date]
    return df_train_val, df_test


def min_max_normalize(df):
    return (df - df.min()) / (df.max() - df.min())


def restore_target(y, df_ref, target_col):
    min_y = df_ref[target_col].min()
    max_y = df_ref[target_col].max()
    return (y * (max_y - min_y)) + min_y


def apply_subsampling(X, y, method):
    if method == "NAIVE":
        # Exemplo: mantém apenas 50% dos exemplos com chuva < 1
        mask = y >= 1
        non_rain_idx = y < 1
        sampled_non_rain_idx = np.random.choice(np.where(non_rain_idx)[0], size=int(len(non_rain_idx) * 0.5), replace=False)
        selected_idx = np.concatenate([np.where(mask)[0], sampled_non_rain_idx])
        return X[selected_idx], y[selected_idx]
    elif method == "NEGATIVE":
        return X[y > 0], y[y > 0]
    return X, y


def generate_windowed_split(df_train, df_val, df_test, target_name, window_size):
    def create_X_y(df):
        X, y = [], []
        for i in range(window_size, len(df)):
            X.append(df.iloc[i-window_size:i].values)
            y.append(df.iloc[i][target_name])
        return np.array(X), np.array(y)

    return (*create_X_y(df_train), *create_X_y(df_val), *create_X_y(df_test))


def build_datasets(station_id: str,
                   input_folder: str,
                   train_start_threshold: datetime.datetime,
                   train_test_threshold: datetime.datetime,
                   test_end_threshold: datetime.datetime,
                   subsampling_procedure: str):

    df = pd.read_parquet(os.path.join(input_folder, f"{station_id}_preprocessed.parquet.gzip"))

    train_start_threshold = pd.to_datetime(train_start_threshold).tz_localize('UTC')
    test_end_threshold = pd.to_datetime(test_end_threshold).tz_localize('UTC')
    if train_start_threshold:
        df = df[df.index >= train_start_threshold]
    if test_end_threshold:
        df = df[df.index <= test_end_threshold]

    df = df[df.index.month.isin([9, 10, 11, 12, 1, 2, 3, 4, 5])]

    min_timestamp, max_timestamp = df.index.min(), df.index.max()

    os.makedirs(globals.DATASETS_DIR, exist_ok=True)
    filename_base = globals.DATASETS_DIR + f"{station_id}"
    df.to_parquet(f"{filename_base}.parquet.gzip", compression='gzip')

    df_train_val, df_test = split_dataframe_by_date(df, train_test_threshold)
    n = len(df_train_val)
    train_cut = int(n * 0.8)
    df_train = df_train_val[:train_cut]
    df_val = df_train_val[train_cut:]

    df_train.to_parquet(f"{filename_base}_train.parquet.gzip")
    df_val.to_parquet(f"{filename_base}_val.parquet.gzip")
    df_test.to_parquet(f"{filename_base}_test.parquet.gzip")

    target_name = "precipitation" 
    df_train = min_max_normalize(df_train)
    df_val = min_max_normalize(df_val)
    df_test = min_max_normalize(df_test)

    with open("config/config.yaml", "r") as f:
        window_size = yaml.safe_load(f)["preproc"]["SLIDING_WINDOW_SIZE"]

    X_train, y_train, X_val, y_val, X_test, y_test = generate_windowed_split(
        df_train, df_val, df_test, target_name, window_size
    )

    y_train = restore_target(y_train, df_train, target_name)
    y_val = restore_target(y_val, df_val, target_name)
    y_test = restore_target(y_test, df_test, target_name)

    if subsampling_procedure != "NONE":
        X_train, y_train = apply_subsampling(X_train, y_train, subsampling_procedure)
        X_val, y_val = apply_subsampling(X_val, y_val, "NEGATIVE")

    with open(f"{filename_base}.pickle", "wb") as f:
        pickle.dump((X_train, y_train, X_val, y_val, X_test, y_test), f)

    logging.info("Processamento concluído com sucesso.")


def main(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--station_id', required=True)
    parser.add_argument('-tt', '--train_test_threshold', required=True)
    parser.add_argument('-b', '--train_start_threshold')
    parser.add_argument('-e', '--test_end_threshold')
    parser.add_argument('-sp', '--subsampling_procedure', default="NONE")

    args = parser.parse_args(argv[1:])
    input_folder = globals.CEMADEN_DATA_DIR + "/preprocessed/"

    try:
        ttt = pd.to_datetime(args.train_test_threshold)
        tst = pd.to_datetime(args.train_start_threshold) if args.train_start_threshold else None
        tet = pd.to_datetime(args.test_end_threshold) if args.test_end_threshold else None
    except Exception as e:
        print("Erro ao converter datas:", str(e))
        parser.print_help()
        sys.exit(1)

    build_datasets(
        station_id=args.station_id,
        input_folder=input_folder,
        train_start_threshold=tst,
        train_test_threshold=ttt,
        test_end_threshold=tet,
        subsampling_procedure=args.subsampling_procedure
    )


if __name__ == "__main__":
    fmt = "[%(levelname)s] %(funcName)s():%(lineno)i: %(message)s"
    logging.basicConfig(level=logging.INFO, format=fmt)
    main(sys.argv)
