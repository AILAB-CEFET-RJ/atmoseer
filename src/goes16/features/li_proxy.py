import logging
import os

import netCDF4 as nc

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not logger.handlers:
    file_handler = logging.FileHandler("li_proxy.log")
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)


def proxy_estabilidade(pasta_c14: str, pasta_c13: str, pasta_saida: str):
    """
    Computes a stability proxy as the difference between channels C14 and C13.

    Args:
        pasta_c14 (str): Path to input files for channel C14.
        pasta_c13 (str): Path to input files for channel C13.
        pasta_saida (str): Path to output directory for LI_proxy feature.
    """
    arquivos_c14 = sorted(os.listdir(pasta_c14))
    arquivos_c13 = sorted(os.listdir(pasta_c13))
    prefix_c14 = arquivos_c14[0].split("_", 1)[0]
    prefix_c13 = arquivos_c13[0].split("_", 1)[0]
    prefix_feature = "LI_proxy"
    arquivos_timestamp = [f.split("_", 1)[1] for f in arquivos_c14 if "_" in f]
    os.makedirs(pasta_saida, exist_ok=True)

    for timestamp in arquivos_timestamp:
        path_c14 = os.path.join(pasta_c14, f"{prefix_c14}_{timestamp}")
        path_c13 = os.path.join(pasta_c13, f"{prefix_c13}_{timestamp}")
        path_saida = os.path.join(pasta_saida, f"{prefix_feature}_{timestamp}")

        if not os.path.exists(path_c14):
            logger.warning(f"Missing file: {path_c14}")
            continue
        if not os.path.exists(path_c13):
            logger.warning(f"Missing file: {path_c13}")
            continue
        if os.path.exists(path_saida):
            continue

        try:
            with (
                nc.Dataset(path_c14, "r") as nc1,
                nc.Dataset(path_c13, "r") as nc2,
                nc.Dataset(path_saida, "w") as out,
            ):
                for nome_dim, dim in nc1.dimensions.items():
                    out.createDimension(
                        nome_dim, len(dim) if not dim.isunlimited() else None
                    )
                vars_saved = 0
                for nome_var in nc1.variables:
                    if nome_var in nc2.variables:
                        dados = nc1.variables[nome_var][:] - nc2.variables[nome_var][:]
                        var_out = out.createVariable(
                            nome_var, "f4", nc1.variables[nome_var].dimensions
                        )
                        var_out[:] = dados
                        var_out.description = "C14 - C13 as a proxy for stability"
                        vars_saved += 1
                if vars_saved == 0:
                    logger.warning(
                        f"No variables saved for {timestamp}, possible mismatch."
                    )
                out.description = "Atmospheric stability proxy from C14 and C13"
        except Exception:
            logger.exception(f"Error processing {timestamp}")
