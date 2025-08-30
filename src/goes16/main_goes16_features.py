import argparse
from config import globals
from goes16.features import (
    profundidade_nuvens,
    glaciacao_topo_nuvem,
    derivada_temporal_fluxo_ascendente,
    gradiente_vapor_agua,
    proxy_estabilidade,
    temperatura_topo_nuvem,
    textura_local_profundidade
)
from goes16.utils import build_channel_paths_by_year, build_feature_paths_by_year
from pathlib import Path

FEATURES_ROOT = Path(globals.GOES16_FEATURES_DIR)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pn", action="store_true")
    parser.add_argument("--gtn", action="store_true")
    parser.add_argument("--fa", action="store_true")
    parser.add_argument("--wv_grad", action="store_true")
    parser.add_argument("--li_proxy", action="store_true")
    parser.add_argument("--toct", action="store_true")
    parser.add_argument("--pn_std", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    if args.pn:
        if args.verbose: print('Feature: pn')
        for c09, c13 in zip(build_channel_paths_by_year("C09"), build_channel_paths_by_year("C13")):
            if args.verbose: print(f'Processing C09: {c09}, C13: {c13}')
            year = c09.parent.name
            profundidade_nuvens(str(c09), str(c13), str(FEATURES_ROOT / "pn" / year))

    if args.gtn:
        if args.verbose: print('Feature: gtn')
        for c11, c14, c15 in zip(build_channel_paths_by_year("C11"), build_channel_paths_by_year("C14"), build_channel_paths_by_year("C15")):
            year = c11.parent.name
            glaciacao_topo_nuvem(str(c11), str(c14), str(c15), str(FEATURES_ROOT / "gtn" / year))

    if args.fa:
        if args.verbose: print('Feature: fa')
        for c13 in build_channel_paths_by_year("C13"):
            year = c13.parent.name
            derivada_temporal_fluxo_ascendente(str(c13), str(FEATURES_ROOT / "fa" / year))

    if args.wv_grad:
        if args.verbose: print('Feature: wv_grad')
        for c09, c08 in zip(build_channel_paths_by_year("C09"), build_channel_paths_by_year("C08")):
            year = c09.parent.name
            gradiente_vapor_agua(str(c09), str(c08), str(FEATURES_ROOT / "wv_grad" / year))

    if args.li_proxy:
        if args.verbose: print('Feature: li_proxy')
        for c14, c13 in zip(build_channel_paths_by_year("C14"), build_channel_paths_by_year("C13")):
            year = c14.parent.name
            proxy_estabilidade(str(c14), str(c13), str(FEATURES_ROOT / "li_proxy" / year))

    if args.toct:
        if args.verbose: print('Feature: toct')
        for c13 in build_channel_paths_by_year("C13"):
            year = c13.parent.name
            temperatura_topo_nuvem(str(c13), str(FEATURES_ROOT / "toct" / year))

    if args.pn_std:
        if args.verbose: print('Feature: pn_std')
        for pn in build_feature_paths_by_year("profundidade_nuvens"):
            year = pn.parent.name
            textura_local_profundidade(str(pn), str(FEATURES_ROOT / "pn_std" / year))


if __name__ == "__main__":
    main()
