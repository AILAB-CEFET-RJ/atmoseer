import os
import netCDF4 as nc
import numpy as np
import logging
from pathlib import Path

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not logger.handlers:
    file_handler = logging.FileHandler("pn.log")
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

def profundidade_nuvens(pasta_entrada_canal9: str, pasta_entrada_canal13: str, pasta_saida: str):
    """
    Calcula a profundidade da nuvem como a diferença entre os canais C09 e C13.

    Args:
        pasta_entrada_canal9 (str): Caminho para os arquivos do canal C09 (por ano).
        pasta_entrada_canal13 (str): Caminho para os arquivos do canal C13 (por ano).
        pasta_saida (str): Caminho de saída para os arquivos PN gerados.
    """
    arquivos_c9 = sorted(os.listdir(pasta_entrada_canal9))
    arquivos_c13 = sorted(os.listdir(pasta_entrada_canal13))

    prefix_c9 = arquivos_c9[0].split("_", 1)[0]
    prefix_c13 = arquivos_c13[0].split("_", 1)[0]
    prefix_feat = "PN"

    timestamps = [f.split("_", 1)[1] for f in arquivos_c9 if "_" in f]
    os.makedirs(pasta_saida, exist_ok=True)

    for ts in timestamps:
        arq_c9 = os.path.join(pasta_entrada_canal9, f"{prefix_c9}_{ts}")
        arq_c13 = os.path.join(pasta_entrada_canal13, f"{prefix_c13}_{ts}")
        arq_out = os.path.join(pasta_saida, f"{prefix_feat}_{ts}")

        if not os.path.exists(arq_c9):
            logger.warning(f"Arquivo ausente: {arq_c9}")
            continue
        if not os.path.exists(arq_c13):
            logger.warning(f"Arquivo ausente: {arq_c13}")
            continue
        if os.path.exists(arq_out):
            continue

        try:
            with nc.Dataset(arq_c9, 'r') as nc1, nc.Dataset(arq_c13, 'r') as nc2, nc.Dataset(arq_out, 'w') as out:
                for nome_dim, dim in nc1.dimensions.items():
                    out.createDimension(nome_dim, len(dim) if not dim.isunlimited() else None)
                vars_salvas = 0
                for nome_var in nc1.variables:
                    if nome_var in nc2.variables:
                        dados = nc1.variables[nome_var][:] - nc2.variables[nome_var][:]
                        var_out = out.createVariable(nome_var, 'f4', nc1.variables[nome_var].dimensions)
                        var_out[:] = dados
                        var_out.description = "PN: profundidade da nuvem (C09 - C13)"
                        vars_salvas += 1
                if vars_salvas == 0:
                    logger.warning(f"Nenhuma variável processada para {ts}, possível incompatibilidade")
                out.description = "Diferença entre canais para profundidade da nuvem"
        except Exception as e:
            logger.exception(f"Erro ao processar {ts}")
