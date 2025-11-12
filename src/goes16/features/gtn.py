import os
import netCDF4 as nc
import numpy as np
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not logger.handlers:
    file_handler = logging.FileHandler("gtn.log")
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

def glaciacao_topo_nuvem(pasta_c11: str, pasta_c14: str, pasta_c15: str, pasta_saida: str):
    """
    Calcula a glaciação do topo da nuvem como uma combinação tri-espectral (C11, C14, C15).

    Args:
        pasta_c11 (str): Caminho para os arquivos do canal C11.
        pasta_c14 (str): Caminho para os arquivos do canal C14.
        pasta_c15 (str): Caminho para os arquivos do canal C15.
        pasta_saida (str): Caminho de saída para os arquivos GTN gerados.
    """
    arquivos_c11 = sorted(os.listdir(pasta_c11))
    arquivos_c14 = sorted(os.listdir(pasta_c14))
    arquivos_c15 = sorted(os.listdir(pasta_c15))

    pfx11 = arquivos_c11[0].split("_", 1)[0]
    pfx14 = arquivos_c14[0].split("_", 1)[0]
    pfx15 = arquivos_c15[0].split("_", 1)[0]
    pfx_feat = 'GTN'

    timestamps = [f.split("_", 1)[1] for f in arquivos_c11 if "_" in f]
    os.makedirs(pasta_saida, exist_ok=True)

    for ts in timestamps:
        arq11 = os.path.join(pasta_c11, f"{pfx11}_{ts}")
        arq14 = os.path.join(pasta_c14, f"{pfx14}_{ts}")
        arq15 = os.path.join(pasta_c15, f"{pfx15}_{ts}")
        arq_out = os.path.join(pasta_saida, f"{pfx_feat}_{ts}")

        if not os.path.exists(arq11):
            logger.warning(f"Arquivo ausente: {arq11}")
            continue
        if not os.path.exists(arq14):
            logger.warning(f"Arquivo ausente: {arq14}")
            continue
        if not os.path.exists(arq15):
            logger.warning(f"Arquivo ausente: {arq15}")
            continue
        if os.path.exists(arq_out):
            continue

        try:
            with nc.Dataset(arq11, 'r') as nc1, nc.Dataset(arq14, 'r') as nc2, nc.Dataset(arq15, 'r') as nc3, nc.Dataset(arq_out, 'w') as out:
                for nome_dim, dim in nc1.dimensions.items():
                    out.createDimension(nome_dim, len(dim) if not dim.isunlimited() else None)
                vars_salvas = 0
                for nome_var in nc1.variables:
                    if nome_var in nc2.variables and nome_var in nc3.variables:
                        dados = (nc1.variables[nome_var][:] - nc2.variables[nome_var][:]) - \
                                (nc2.variables[nome_var][:] - nc3.variables[nome_var][:])
                        var_out = out.createVariable(nome_var, 'f4', nc1.variables[nome_var].dimensions)
                        var_out[:] = dados
                        var_out.description = 'GTN: glaciação topo da nuvem (tri-espectral)'
                        vars_salvas += 1
                if vars_salvas == 0:
                    logger.warning(f"Nenhuma variável processada para {ts}, possível incompatibilidade")
                out.description = "Glaciação topo da nuvem (GTN)"
        except Exception as e:
            logger.exception(f"Erro ao processar {ts}")
