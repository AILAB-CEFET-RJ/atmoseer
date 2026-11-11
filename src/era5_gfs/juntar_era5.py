#!/usr/bin/env python
"""ETAPA 2 -- juntar. era5-bruto/<conjunto>/<var>/RJ_<ano>_<mes>.nc  ->
                     era5-<conjunto>-levels-conv/RJ_<ano>_<mes>.nc

A saida tem exatamente o formato que o notebook de construcao le
(stconvs2s-era5-data-*.ipynb, load_merged_era5): coordenadas valid_time,
latitude, longitude e pressure_level.

Nunca aborta. Mes com variavel faltando ou invalida NAO e gravado (o
open_mfdataset do notebook exige o mesmo conjunto de variaveis em todos os
meses) e aparece no relatorio com o nome do que falta. --parcial grava mesmo
assim.

Uso (no env `era5`):
    python src/era5_gfs/juntar_era5.py
    python src/era5_gfs/juntar_era5.py --anos 2020 --meses 1
    python src/era5_gfs/juntar_era5.py --parcial
"""
from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

import xarray as xr

import era5_catalogo as C
import era5_comum as M


def log(msg: str) -> None:
    print(msg, flush=True)


def juntar_mes(raiz: Path, conjunto: str, ano: int, mes: int, vars_: list[C.Var],
               parcial: bool, refazer: bool) -> tuple[str, list[str]]:
    """Devolve (situacao, problemas). situacao: gravado | atualizado | incompleto | vazio."""
    presentes, problemas, fontes = [], [], []
    for v in vars_:
        p = M.caminho_bruto(raiz, v, ano, mes)
        if not p.exists():
            problemas.append(f"{v.curto}: falta")
            continue
        try:
            with xr.open_dataset(p) as ds:
                da = ds[v.curto].load()
        except Exception as exc:                              # noqa: BLE001
            problemas.append(f"{v.curto}: ilegivel ({type(exc).__name__}: {exc})")
            continue
        erros = M.validar(da, v, ano, mes)
        if erros:
            problemas.append(f"{v.curto}: " + "; ".join(erros))
            continue
        presentes.append(da)
        fontes.append(p)

    if problemas and not parcial:
        return "incompleto", problemas
    if not presentes:
        return "vazio", problemas

    destino = M.caminho_final(raiz, conjunto, ano, mes)
    if destino.exists() and not refazer:
        with xr.open_dataset(destino) as ds:
            mesmas = set(ds.data_vars) == {da.name for da in presentes}
        if mesmas and destino.stat().st_mtime >= max(f.stat().st_mtime for f in fontes):
            return "atualizado", problemas

    ds = xr.merge(presentes, compat="override", join="exact")
    destino.parent.mkdir(parents=True, exist_ok=True)
    tmp = destino.with_suffix(".nc.part")
    ds.to_netcdf(tmp)
    tmp.replace(destino)
    return "gravado", problemas


def juntar_estaticas(raiz: Path) -> list[str]:
    saidas = []
    for v in C.VARIAVEIS:
        if not v.estatica:
            continue
        origem = M.caminho_estatico(raiz, v)
        if not origem.exists():
            log(f"  estatico {v.curto}: ainda nao baixado")
            continue
        nome = "orografia.nc" if (v.conjunto, v.curto) == ("single", "z") else f"{v.conjunto}_{v.curto}.nc"
        destino = Path(raiz) / M.DIR_ESTATICO / nome
        destino.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(origem, destino)
        saidas.append(str(destino.relative_to(raiz)))
    return saidas


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--conjunto", choices=["single", "pressure", "ambos"], default="ambos")
    ap.add_argument("--anos", type=int, nargs="+", default=None)
    ap.add_argument("--meses", type=int, nargs="+", default=None)
    ap.add_argument("--parcial", action="store_true",
                    help="grava o mes mesmo com variaveis faltando (o notebook pode estranhar)")
    ap.add_argument("--refazer", action="store_true", help="regrava mesmo se a saida estiver em dia")
    ap.add_argument("--raiz", type=Path, default=M.RAIZ_PADRAO)
    a = ap.parse_args(argv)

    meses = C.periodo(a.anos, a.meses)
    conjuntos = ["single", "pressure"] if a.conjunto == "ambos" else [a.conjunto]
    incompletos = 0
    for conjunto in conjuntos:
        vars_ = C.selecionar(conjunto, incluir_estaticas=False)
        log(f"\n=== {conjunto.upper()}  ({len(vars_)} variaveis por mes) -> {M.DIR_FINAL[conjunto]}/")
        contagem = {}
        for ano, mes in meses:
            situacao, problemas = juntar_mes(a.raiz, conjunto, ano, mes, vars_, a.parcial, a.refazer)
            contagem[situacao] = contagem.get(situacao, 0) + 1
            if situacao == "incompleto":
                incompletos += 1
                faltam = [p.split(":")[0] for p in problemas if p.endswith(": falta")]
                outros = [p for p in problemas if not p.endswith(": falta")]
                msg = f"  {ano}-{mes:02d}: NAO gravado"
                if faltam:
                    msg += f" -- faltam {faltam}" if len(faltam) <= 8 else f" -- faltam {len(faltam)} variaveis"
                log(msg)
                for p in outros:
                    log(f"           invalido: {p}")
            elif situacao == "gravado":
                extra = f"  (parcial: {len(problemas)} problema(s))" if problemas else ""
                log(f"  {ano}-{mes:02d}: gravado{extra}")
        log("  " + ", ".join(f"{k}: {n}" for k, n in sorted(contagem.items())))

    est = juntar_estaticas(a.raiz)
    if est:
        log(f"\nestaticos: {', '.join(est)}")
    if incompletos:
        log(f"\n{incompletos} mes(es) incompleto(s). Veja o que falta com:\n"
            f"    python src/era5_gfs/baixar_era5.py --status\n"
            f"e rode baixar_era5.py de novo -- ele so baixa o que falta.")
    return 1 if incompletos else 0


if __name__ == "__main__":
    sys.exit(main())
