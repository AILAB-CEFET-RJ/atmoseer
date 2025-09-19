import os
import argparse
from pathlib import Path
import netCDF4 as nc
import matplotlib.pyplot as plt
import numpy as np

# Mapeamento de sigla para pasta e prefixo do arquivo
FEATURE_MAP = {
    "pn": ("profundidade_nuvens", "PN"),
    "pnstd": ("textura_local_profundidade", "PNstd"),
    "toct": ("temperatura_topo_nuvem", "TOCT"),
    "c07": ("tamanho_particulas", "C07"),
    "fa": ("movimento_vertical", "FA"),
    "li_proxy": ("li_proxy", "LIPROXY"),
    "wv_grad": ("gradiente_vapor_agua", "WVgrad"),
    "gtn": ("glaciacao_topo_nuvem", "GTN"),
}

FEATURES_DIR = Path("features")


def create_snapshots(netcdf_file, output_directory, title_prefix=""):
    os.makedirs(output_directory, exist_ok=True)

    with nc.Dataset(netcdf_file, 'r') as ds:
        variaveis = ds.variables.keys()
        
        for var in variaveis:
            data = ds.variables[var][:]
            
            if data.ndim != 2:
                print(f"[Ignorado] '{var}' não é 2D (shape: {data.shape})")
                continue

            plt.figure(figsize=(8, 6))
            plt.imshow(data, cmap='viridis', origin='upper')
            plt.colorbar(label='Valor')
            plt.title(f"{title_prefix} {var}")

            nome_limpo = var.replace(":", "_").replace(" ", "_")
            caminho_saida = os.path.join(output_directory, f"{nome_limpo}.png")
            plt.savefig(caminho_saida, bbox_inches='tight')
            plt.close()
            print(f"[Salvo] {caminho_saida}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--feature", required=True, help="Sigla da feature (ex: pn, pnstd, toct, etc)")
    parser.add_argument("--date", required=True, help="Data no formato YYYY_MM_DD")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    feature_key = args.feature.lower()
    date_str = args.date

    if feature_key not in FEATURE_MAP:
        print(f"[Erro] Feature '{feature_key}' não reconhecida.")
        print("Features disponíveis:", ', '.join(FEATURE_MAP.keys()))
        return

    pasta, prefixo = FEATURE_MAP[feature_key]
    ano = date_str.split("_")[0]

    file_path = FEATURES_DIR / pasta / ano / f"{prefixo}_{date_str}.nc"

    if not file_path.exists():
        print(f"[Não encontrado] Arquivo: {file_path}")
        return

    if args.verbose:
        print(f"[Processando] {file_path}")

    output_dir = f"data/goes16/plots/{feature_key}/{date_str}"
    create_snapshots(str(file_path), output_dir, title_prefix=prefixo)


if __name__ == "__main__":
    main()
