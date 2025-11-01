import logging
import os

import netCDF4 as nc
import numpy as np
from scipy.ndimage import generic_filter

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not logger.handlers:
    file_handler = logging.FileHandler("pn_std.log")
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)


def textura_local_profundidade(
    pasta_pn: str, pasta_saida: str, tamanho_janela: int = 3
):
    """
    Calcula o desvio padrão local (textura) da profundidade da nuvem (PN).

    Args:
        pasta_pn (str): Caminho para os arquivos da feature PN.
        pasta_saida (str): Caminho de saída para os arquivos PNstd gerados.
        tamanho_janela (int): Tamanho da janela para o filtro de desvio padrão.
    """
    arquivos = sorted(os.listdir(pasta_pn))
    if not arquivos:
        logger.warning(f"Nenhum arquivo encontrado em {pasta_pn}")
        return

    prefix = arquivos[0].split("_", 1)[0]
    prefix_feature = "PNstd"
    arquivos_timestamp = [f.split("_", 1)[1] for f in arquivos if "_" in f]
    os.makedirs(pasta_saida, exist_ok=True)

    for timestamp in arquivos_timestamp:
        path_entrada = os.path.join(pasta_pn, f"{prefix}_{timestamp}")
        path_saida = os.path.join(pasta_saida, f"{prefix_feature}_{timestamp}")

        if not os.path.exists(path_entrada):
            logger.warning(f"Arquivo ausente: {path_entrada}")
            continue
        if os.path.exists(path_saida):
            continue

        try:
            with (
                nc.Dataset(path_entrada, "r") as src,
                nc.Dataset(path_saida, "w") as dst,
            ):
                for nome_dim, dim in src.dimensions.items():
                    dst.createDimension(
                        nome_dim, len(dim) if not dim.isunlimited() else None
                    )
                vars_salvas = 0
                for nome_var in src.variables:
                    dados = src.variables[nome_var][:]
                    std_local = generic_filter(
                        dados, np.std, size=tamanho_janela, mode="nearest"
                    )
                    var_out = dst.createVariable(
                        nome_var, "f4", src.variables[nome_var].dimensions
                    )
                    var_out[:] = std_local
                    var_out.description = "Desvio padrão espacial local (textura) da profundidade da nuvem"
                    vars_salvas += 1
                if vars_salvas == 0:
                    logger.warning(
                        f"Nenhuma variável de textura gerada para {timestamp}"
                    )
                dst.description = "Textura espacial (desvio padrão local) da feature PN"
        except Exception:
            logger.exception(f"Erro ao processar {timestamp}")
