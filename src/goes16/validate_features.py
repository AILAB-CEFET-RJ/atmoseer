# validate_features.py
import os
import argparse
import netCDF4 as nc
from pathlib import Path
from collections import defaultdict

def list_timestamps_from_dir(path):
    arquivos = sorted(os.listdir(path))
    return set(f.split("_", 1)[1] for f in arquivos if "_" in f)

def validate_presence(feature_dir, canais, base_raw_dir="data/goes16/CMI"):
    print(f"\n📁 Validando presença de arquivos em: {feature_dir}")
    for year_dir in sorted(Path(feature_dir).iterdir()):
        if not year_dir.is_dir():
            continue
        ano = year_dir.name
        feature_files = list_timestamps_from_dir(year_dir)

        expected = None
        for canal in canais:
            path = Path(base_raw_dir) / ano / canal
            if not path.exists():
                print(f"  ⚠ Referência ausente: {path}")
                continue
            ref_files = list_timestamps_from_dir(path)
            if expected is None:
                expected = ref_files
            else:
                expected &= ref_files

        if expected is None:
            print(f"  ⚠ Nenhum diretório de referência disponível para {ano}")
            continue

        diff = expected - feature_files
        if not diff:
            print(f"  ✓ {ano}: {len(feature_files)} arquivos gerados corretamente")
        else:
            print(f"  ✗ {ano}: {len(diff)} arquivos ausentes. Ex: {list(diff)[:3]}")

def validate_netcdf_structure(feature_dir):
    print(f"\n🔍 Validando estrutura NetCDF em: {feature_dir}")
    problemas = defaultdict(list)
    for year_dir in sorted(Path(feature_dir).iterdir()):
        if not year_dir.is_dir():
            continue
        for f in sorted(year_dir.glob("*.nc")):
            try:
                with nc.Dataset(f, 'r') as ds:
                    if not ds.variables:
                        problemas['sem_variaveis'].append(f.name)
                    for v in ds.variables.values():
                        if not hasattr(v, 'description'):
                            problemas['sem_descricao'].append(f.name)
            except Exception:
                problemas['erro_abertura'].append(f.name)
    if not problemas:
        print("  ✓ Todos os arquivos têm estrutura válida.")
    else:
        for tipo, arqs in problemas.items():
            print(f"  ✗ {tipo}: {len(arqs)} arquivos. Ex: {arqs[:3]}")

def run_all(feature: str, canais: list):
    feature_path = Path("data/goes16/features") / feature
    validate_presence(str(feature_path), canais)
    validate_netcdf_structure(str(feature_path))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Valida arquivos de features GOES-16")
    parser.add_argument("--feature", required=True, help="Nome da feature (ex: pn, gtn, wv_grad)")
    parser.add_argument("--canais", nargs='+', required=True, help="Lista de canais de referência (ex: C09 C13)")
    args = parser.parse_args()

    run_all(args.feature, args.canais)
