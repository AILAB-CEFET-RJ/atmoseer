import os
import netCDF4 as nc
import numpy as np

def temperatura_topo_nuvem(pasta_c13: str, pasta_saida: str):
    """
    Extrai a temperatura do topo da nuvem diretamente do canal C13.

    Args:
        pasta_c13 (str): Caminho para os arquivos do canal C13.
        pasta_saida (str): Caminho de saída para os arquivos TOCT gerados.
    """
    arquivos = sorted(os.listdir(pasta_c13))
    prefix = arquivos[0].split("_", 1)[0]
    prefix_feature = 'TOCT'
    arquivos_timestamp = [f.split("_", 1)[1] for f in arquivos if "_" in f]
    os.makedirs(pasta_saida, exist_ok=True)

    for timestamp in arquivos_timestamp:
        path_entrada = os.path.join(pasta_c13, f"{prefix}_{timestamp}")
        path_saida = os.path.join(pasta_saida, f"{prefix_feature}_{timestamp}")

        if not os.path.exists(path_saida):
            with nc.Dataset(path_entrada, 'r') as src, nc.Dataset(path_saida, 'w') as dst:
                for nome_dim, dim in src.dimensions.items():
                    dst.createDimension(nome_dim, len(dim) if not dim.isunlimited() else None)
                for nome_var in src.variables:
                    dados = src.variables[nome_var][:]
                    var_out = dst.createVariable(nome_var, 'f4', src.variables[nome_var].dimensions)
                    var_out[:] = dados
                    var_out.description = 'Temperatura do topo da nuvem (TOCT) diretamente do canal 13'
                dst.description = 'TOCT a partir do canal C13'
