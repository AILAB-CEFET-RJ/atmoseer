import os
import netCDF4 as nc
import logging
from pathlib import Path

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not logger.handlers:
    file_handler = logging.FileHandler("tp.log")
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

def tamanho_particulas(pasta_entrada_canal7: str, pasta_saida: str):
    """
    Mapeia o tamanho das partículas (TP) diretamente a partir dos arquivos do canal C07.

    Args:
        pasta_entrada_canal7 (str): Caminho para os arquivos do canal C07 (por ano).
        pasta_saida (str): Caminho de saída para os arquivos TP gerados.
    """
    arquivos_c7 = sorted(os.listdir(pasta_entrada_canal7))
    prefix_c7 = arquivos_c7[0].split("_", 1)[0]
    prefix_feat = "TP"

    timestamps = [f.split("_", 1)[1] for f in arquivos_c7 if "_" in f]
    os.makedirs(pasta_saida, exist_ok=True)

    for ts in timestamps:
        arq_c7 = os.path.join(pasta_entrada_canal7, f"{prefix_c7}_{ts}")
        arq_out = os.path.join(pasta_saida, f"{prefix_feat}_{ts}")

        if not os.path.exists(arq_c7):
            logger.warning(f"Arquivo ausente: {arq_c7}")
            continue
        if os.path.exists(arq_out):
            continue

        try:
            with nc.Dataset(arq_c7, 'r') as nc_in, nc.Dataset(arq_out, 'w') as out:
                for nome_dim, dim in nc_in.dimensions.items():
                    out.createDimension(nome_dim, len(dim) if not dim.isunlimited() else None)

                for nome_var in nc_in.variables:
                    var_in = nc_in.variables[nome_var]
                    var_out = out.createVariable(nome_var, var_in.datatype, var_in.dimensions)
                    var_out.setncatts({k: var_in.getncattr(k) for k in var_in.ncattrs()})
                    var_out[:] = var_in[:]

                out.description = "TP: tamanho das partículas a partir do canal C07"
        except Exception as e:
            logger.exception(f"Erro ao processar {ts}")
