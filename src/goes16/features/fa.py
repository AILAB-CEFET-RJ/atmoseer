import os
import netCDF4 as nc
import numpy as np
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not logger.handlers:
    file_handler = logging.FileHandler("fa.log")
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

def derivada_temporal_fluxo_ascendente(pasta_entrada_canal: str, pasta_saida: str, intervalo_temporal: int = 10):
    """
    Calcula a derivada temporal do fluxo ascendente com base no canal fornecido.

    Args:
        pasta_entrada_canal (str): Caminho para os arquivos do canal.
        pasta_saida (str): Caminho de saída para os arquivos FA gerados.
        intervalo_temporal (int): Intervalo em minutos entre frames (default: 10).
    """
    arquivos = sorted(os.listdir(pasta_entrada_canal))
    if not arquivos:
        logger.warning(f"Nenhum arquivo encontrado em {pasta_entrada_canal}")
        return

    pfx = arquivos[0].split("_", 1)[0]
    timestamps = [f.split("_", 1)[1] for f in arquivos if "_" in f]
    os.makedirs(pasta_saida, exist_ok=True)

    for ts in timestamps:
        arq_atual = os.path.join(pasta_entrada_canal, f"{pfx}_{ts}")
        arq_saida = os.path.join(pasta_saida, f"FA_{ts}")

        if not os.path.exists(arq_atual):
            logger.warning(f"Arquivo ausente: {arq_atual}")
            continue
        if os.path.exists(arq_saida):
            continue

        try:
            with nc.Dataset(arq_atual, 'r') as src, nc.Dataset(arq_saida, 'w') as dst:
                for nome_dim, dim in src.dimensions.items():
                    dst.createDimension(nome_dim, len(dim) if not dim.isunlimited() else None)
                vars_salvas = 0
                for nome_var in src.variables:
                    partes = nome_var.split('_')[1:]
                    try:
                        year, mes, dia, hora, minuto = map(int, partes)
                        hora_adiante = datetime(year, mes, dia, hora, minuto) + timedelta(minutes=intervalo_temporal)
                        nome_adiante = f"CMI_{hora_adiante.year:04}_{hora_adiante.month:02}_{hora_adiante.day:02}_{hora_adiante.hour:02}_{hora_adiante.minute:02}"
                        if nome_adiante in src.variables:
                            delta_t = intervalo_temporal
                            derivada = (src.variables[nome_adiante][:] - src.variables[nome_var][:]) / delta_t
                            var_out = dst.createVariable(nome_var, 'f4', src.variables[nome_var].dimensions)
                            var_out[:] = derivada
                            var_out.description = f"Derivada temporal de {nome_var}"
                            vars_salvas += 1
                    except ValueError:
                        continue
                if vars_salvas == 0:
                    logger.warning(f"Nenhuma variável derivada para {ts}, possível falha de naming")
                dst.description = "Derivada temporal do fluxo ascendente"
        except Exception as e:
            logger.exception(f"Erro ao processar {ts}")
