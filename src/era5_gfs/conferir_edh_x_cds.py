#!/usr/bin/env python
"""Confere se o Earth Data Hub entrega o MESMO dado que o CDS.

Usa como gabarito os arquivos de 2020-01 baixados do CDS em set/2026
(era5-bruto/_gabarito_cds/). Compara grade, horas, unidades e a convencao de
acumulacao do tp. Se bater, o espelho e confiavel para o lote inteiro.

O EDH NAO e copia bit a bit: ele comprime com arredondamento de bits e guarda
10 bits de mantissa (medido em 2026-10-05: os 13 bits finais vem zerados em
todas as variaveis). O erro relativo maximo desse arredondamento e 2**-11 =
0,000488, sem vies. Entao o criterio aqui e esse limite, nao igualdade exata:
um erro de grade, de hora ou de unidade daria diferencas ordens de grandeza
maiores e seria pego do mesmo jeito.

Rodar DEPOIS de:
    python src/era5_gfs/baixar_era5.py --anos 2020 --meses 1 --vars cape cin tcwv vimdf sp tp
"""
import sys
from pathlib import Path

import numpy as np
import xarray as xr

import baixar_era5 as B
import era5_catalogo as C
import era5_comum as M

GABARITO = {
    "cds_2020_1_cape_cin_tcwv_vimdf_sp.nc": ["cape", "cin", "tcwv", "vimdf", "sp"],
    "cds_2020_1_tp.nc": ["tp"],
}


# erro relativo maximo do arredondamento a 10 bits de mantissa, com folga de
# 1% para o arredondamento do proprio gabarito do CDS
LIMITE_REL = 2.0 ** -11 * 1.01


def _veio_do_cds(raiz, var) -> bool:
    """A ultima gravacao desta variavel em 2020-01 veio do CDS (fallback)?"""
    import json
    reg = raiz / M.DIR_BRUTO / "_registro.jsonl"
    fonte = None
    if reg.exists():
        for linha in reg.read_text().splitlines():
            r = json.loads(linha)
            if (r["var"], r["ano"], r["mes"], r["status"]) == (var.curto, 2020, 1, "ok"):
                fonte = r["fonte"]
    return fonte == "cds"


def main() -> int:
    raiz = M.RAIZ_PADRAO
    pasta = raiz / M.DIR_BRUTO / "_gabarito_cds"
    ruins = 0
    print(f"{'var':<6} {'max |EDH-CDS|':>14} {'escala':>11} {'NaN iguais':>11}  veredito")
    for arquivo, nomes in GABARITO.items():
        cds = M.recortar(B.abrir_resposta(pasta / arquivo))
        for nome in nomes:
            var = next(v for v in C.VARIAVEIS if v.conjunto == "single" and v.curto == nome)
            p = M.caminho_bruto(raiz, var, 2020, 1)
            if not p.exists():
                print(f"{nome:<6} {'':>14} {'':>11} {'':>11}  NAO BAIXADO do EDH ainda")
                ruins += 1
                continue
            with xr.open_dataset(p) as ds:
                edh = ds[nome].load()
            a, b = edh.values.astype("f8"), cds[nome].values.astype("f8")
            if a.shape != b.shape:
                print(f"{nome:<6}  formas diferentes: EDH {a.shape} x CDS {b.shape}")
                ruins += 1
                continue
            if var.fontes[0] != "edh" or _veio_do_cds(raiz, var):
                print(f"{nome:<6} {'':>14} {'':>11} {'':>11}  veio do CDS -- nao testa o EDH")
                continue
            nan_iguais = bool((np.isnan(a) == np.isnan(b)).all())
            m = ~np.isnan(a) & ~np.isnan(b)
            dif = float(np.abs(a[m] - b[m]).max()) if m.any() else 0.0
            escala = float(np.abs(b[m]).max()) if m.any() else 0.0
            nz = m & (b != 0)
            rel = float((np.abs(a[nz] - b[nz]) / np.abs(b[nz])).max()) if nz.any() else 0.0
            zeros_iguais = bool(((a == 0) == (b == 0))[m].all())
            ok = nan_iguais and zeros_iguais and rel <= LIMITE_REL
            ruins += not ok
            print(f"{nome:<6} {dif:14.4g} {escala:11.4g} {str(nan_iguais):>11}  "
                  f"{'OK' if ok else 'DIFERENTE -- nao seguir com o lote'}"
                  f"  (erro rel. max {rel:.6f}, limite {LIMITE_REL:.6f})")
    print("\nO EDH bate com o CDS." if ruins == 0 else f"\n{ruins} variavel(is) com problema.")
    return 0 if ruins == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
