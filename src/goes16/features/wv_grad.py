import os
import netCDF4 as nc
import numpy as np
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not logger.handlers:
    file_handler = logging.FileHandler("wv_grad.log")
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

def gradiente_vapor_agua(pasta_c09: str, pasta_c08: str, pasta_saida: str):
    """
    Calcula o gradiente espectral de vapor d'água como a diferença entre os canais C09 e C08.

    Args:
        pasta_c09 (str): Caminho para os arquivos do canal C09.
        pasta_c08 (str): Caminho para os arquivos do canal C08.
        pasta_saida (str): Caminho de saída para os arquivos WV_grad gerados.
    """
    arquivos_c09 = sorted(os.listdir(pasta_c09))
    arquivos_c08 = sorted(os.listdir(pasta_c08))
    prefix_c09 = arquivos_c09[0].split("_", 1)[0]
    prefix_c08 = arquivos_c08[0].split("_", 1)[0]
    prefix_feature = 'WV_grad'
    arquivos_timestamp = [f.split("_", 1)[1] for f in arquivos_c09 if "_" in f]
    os.makedirs(pasta_saida, exist_ok=True)

    for timestamp in arquivos_timestamp:
        path_c09 = os.path.join(pasta_c09, f'{prefix_c09}_{timestamp}')
        path_c08 = os.path.join(pasta_c08, f'{prefix_c08}_{timestamp}')
        path_saida = os.path.join(pasta_saida, f'{prefix_feature}_{timestamp}')

        if not os.path.exists(path_c09):
            logger.warning(f"Arquivo ausente: {path_c09}")
            continue
        if not os.path.exists(path_c08):
            logger.warning(f"Arquivo ausente: {path_c08}")
            continue
        if os.path.exists(path_saida):
            continue

        try:
            with nc.Dataset(path_c09, 'r') as nc1, nc.Dataset(path_c08, 'r') as nc2, nc.Dataset(path_saida, 'w') as out:
                for nome_dim, dim in nc1.dimensions.items():
                    out.createDimension(nome_dim, len(dim) if not dim.isunlimited() else None)
                vars_salvas = 0
                for nome_var in nc1.variables:
                    if nome_var in nc2.variables:
                        dados = nc1.variables[nome_var][:] - nc2.variables[nome_var][:]
                        var_out = out.createVariable(nome_var, 'f4', nc1.variables[nome_var].dimensions)
                        var_out[:] = dados
                        var_out.description = f'Diferença C09 - C08 (vapor d’água)'
                        vars_salvas += 1
                if vars_salvas == 0:
                    logger.warning(f"Nenhuma variável processada para {timestamp}, possível incompatibilidade")
                out.description = 'Gradiente espectral do vapor d’água'
        except Exception as e:
            logger.exception(f"Erro ao processar {timestamp}")
