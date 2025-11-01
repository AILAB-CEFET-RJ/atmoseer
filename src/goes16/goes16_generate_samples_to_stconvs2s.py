import glob
import re

import numpy as np
import xarray as xr


def process_single_day(files):
    """
    Processa os arquivos de um único dia e retorna um dataset combinado.
    """
    daily_datasets = []

    for file_path in files:
        ds = xr.open_dataset(file_path)

        time_stamps = []
        data_arrays = []

        for var_name in ds.variables:
            match = re.search(r"CMI_(\d{4}_\d{2}_\d{2}_\d{2}_\d{2})", var_name)
            if match:
                timestamp_str = match.group(1)
                timestamp = np.datetime64(
                    f"{timestamp_str[:4]}-{timestamp_str[5:7]}-{timestamp_str[8:10]}T{timestamp_str[11:13]}:{timestamp_str[14:]}"
                )
                time_stamps.append(timestamp)

                var_data = ds[var_name].rename(
                    {f"dim_0_{var_name}": "lat", f"dim_1_{var_name}": "lon"}
                )
                data_arrays.append(var_data)

        time_array = xr.DataArray(
            np.array(time_stamps, dtype="datetime64[ns]"), dims="time"
        )
        daily_data = xr.concat(data_arrays, dim=time_array)

        daily_data = daily_data.assign_coords(
            lat=np.arange(daily_data.sizes["lat"]),
            lon=np.arange(daily_data.sizes["lon"]),
        )

        daily_datasets.append(daily_data)

    # Concatenar todos os datasets do dia ao longo da dimensão 'time'
    combined_day = xr.concat(daily_datasets, dim="time")
    combined_day = combined_day.sortby("time")  # Ordenar por timestamps

    return combined_day


def collect_samples(combined_data, TIMESTEP, max_gap):
    """
    Coleta os samples de um dataset baseado em janelas de tempo e considera todas as bandas.

    Parâmetros:
        combined_data: xarray.Dataset - Dataset combinado com as dimensões `time` e `channel`.
        TIMESTEP: int - Número de timestamps por sample.
        max_gap: int - Máximo intervalo permitido entre timestamps consecutivos (em minutos).

    Retorna:
        xarray.Dataset - Dataset contendo os samples X e Y.
    """
    total_time = combined_data.sizes["time"]

    X_samples = []
    Y_samples = []

    for i in range(total_time - TIMESTEP):
        # Coleta do sample X
        X_sample = combined_data.isel(time=slice(i, i + TIMESTEP)).assign_coords(
            time=combined_data.time.isel(time=slice(i, i + TIMESTEP))
        )
        max_gap_X = check_max_gap(X_sample)

        # Coleta do sample Y
        Y_sample = combined_data.isel(
            time=slice(i + 1, i + 1 + TIMESTEP)
        ).assign_coords(
            time=combined_data.time.isel(time=slice(i + 1, i + 1 + TIMESTEP))
        )
        max_gap_Y = check_max_gap(Y_sample)

        # Verificar gaps
        if max_gap_X > max_gap:
            print(f"Timestamp faltando no X_sample: {X_sample.time.values}")
            continue
        elif max_gap_Y > max_gap:
            print(f"Timestamp faltando no Y_sample: {Y_sample.time.values}")
            continue

        # Adicionar os samples válidos
        X_samples.append(X_sample)
        Y_samples.append(Y_sample)

    # Concatenar os samples ao longo da dimensão 'sample'
    X_samples = xr.concat(X_samples, dim="sample")
    Y_samples = xr.concat(Y_samples, dim="sample")

    # Dataset final contendo X e Y
    combined_samples = xr.Dataset({"x": X_samples, "y": Y_samples})

    return combined_samples


def check_max_gap(sample):
    """
    Calcula a maior diferença entre timestamps consecutivos dentro de um sample.

    Parâmetros:
        sample: xarray.Dataset - Dataset contendo a dimensão `time`.

    Retorna:
        int - O maior intervalo de tempo (em minutos) entre timestamps consecutivos.
    """
    times = sample.time.values
    gaps = np.diff(times).astype("timedelta64[m]").astype(int)  # Diferenças em minutos
    return np.max(gaps) if len(gaps) > 0 else 0


def main(path, max_gap, bands, output):
    TIMESTEP = 5
    files = glob.glob(path)
    files.sort()

    # Organizar arquivos por dia
    days = {}
    for file in files:
        day = re.search(r"(\d{4}_\d{2}_\d{2})", file).group(1)
        if day not in days:
            days[day] = []
        days[day].append(file)

    # Processar cada dia
    for day, day_files in days.items():
        print(f"Processando o dia: {day}")

        band_datasets = []
        for band in bands:
            band_files = [file for file in day_files if f"band{band}" in file]
            if not band_files:
                print(f"Sem arquivos encontrados para a banda {band} no dia {day}")
                continue

            daily_dataset = process_single_day(band_files)
            band_datasets.append(daily_dataset)

        if band_datasets:
            combined_day = xr.concat(band_datasets, dim="channel")
            combined_day = combined_day.assign_coords(channel=("channel", bands))

            # Coletar samples do dia
            daily_samples = collect_samples(combined_day, TIMESTEP, max_gap)

            # Salvar os samples por dia
            daily_output_path = f"{output}/{day}_samples.nc"
            daily_samples.to_netcdf(daily_output_path)
            print(f"Samples do dia {day} salvos em {daily_output_path}")
        else:
            print(f"Nenhum dataset processado para o dia {day}.")


path = "./features/CMI/"
max_gap = 30
features = ["dF_dt"]
output = "output"
main(path, max_gap, features, output)
