"""Mecanica compartilhada por baixar_era5.py e juntar_era5.py.

O QUE baixar esta em era5_catalogo.py. Aqui so ha o COMO: recortar o dominio,
padronizar nomes de coordenadas, validar um arquivo e grava-lo sem deixar
lixo pela metade.
"""
from __future__ import annotations

import calendar
import json
import threading
import time
from datetime import datetime
import os
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

import era5_catalogo as C

# Onde ficam era5-bruto/, era5-*-conv/ etc. Padrao: o diretorio atual, ou a
# variavel ERA5_RAIZ. Cada script aceita tambem --raiz.
RAIZ_PADRAO = Path(os.environ.get("ERA5_RAIZ", ".")).resolve()
DIR_BRUTO = "era5-bruto"
DIR_ESTATICO = "era5-estatico"
DIR_FINAL = {"single": "era5-single-levels-conv", "pressure": "era5-pressure-levels-conv"}

_NOMES_TEMPO = ("valid_time", "time")
_NOMES_LAT = ("latitude", "lat")
_NOMES_LON = ("longitude", "lon")
_NOMES_NIVEL = ("pressure_level", "level", "isobaricInhPa")


# --------------------------------------------------------------------- caminhos
def caminho_bruto(raiz: Path, var: C.Var, ano: int, mes: int) -> Path:
    return Path(raiz) / DIR_BRUTO / var.conjunto / var.curto / f"RJ_{ano}_{mes}.nc"


def caminho_estatico(raiz: Path, var: C.Var) -> Path:
    return Path(raiz) / DIR_BRUTO / var.conjunto / var.curto / "estatico.nc"


def caminho_final(raiz: Path, conjunto: str, ano: int, mes: int) -> Path:
    return Path(raiz) / DIR_FINAL[conjunto] / f"RJ_{ano}_{mes}.nc"


# ------------------------------------------------------------------------ grade
def grade_esperada() -> tuple[np.ndarray, np.ndarray]:
    lat = np.round(np.arange(C.GRID_NORTH, C.GRID_SOUTH - 1e-9, -C.RESOLUTION), 4)
    lon = np.round(np.arange(C.GRID_WEST, C.GRID_EAST + 1e-9, C.RESOLUTION), 4)
    return lat, lon


def horas_do_mes(ano: int, mes: int) -> pd.DatetimeIndex:
    dias = calendar.monthrange(ano, mes)[1]
    return pd.date_range(f"{ano}-{mes:02d}-01", periods=dias * 24, freq="h")


def _achar(obj, candidatos, rotulo):
    for n in candidatos:
        if n in obj.dims or n in obj.coords:
            return n
    raise KeyError(f"sem coordenada de {rotulo} (procurei {candidatos}; "
                   f"tem {list(obj.dims)})")


# ------------------------------------------------------------------- recortar
def recortar(obj, niveis: list[int] | None = None, inicio=None, fim=None):
    """Recorta dominio, niveis e tempo, e padroniza os nomes de coordenada.

    Funciona em arrays PREGUICOSOS e globais (EDH): os indices sao calculados
    nas coordenadas, e so o pedaco selecionado e lido. Saida sempre com
    `valid_time`, `latitude` (norte -> sul), `longitude` (-180..180, oeste ->
    leste) e, se houver, `pressure_level` na ordem de NIVEIS.
    """
    t, la, lo = (_achar(obj, _NOMES_TEMPO, "tempo"), _achar(obj, _NOMES_LAT, "latitude"),
                 _achar(obj, _NOMES_LON, "longitude"))

    lat_v = obj[la].values
    lon_v = obj[lo].values
    lon_180 = ((lon_v + 180.0) % 360.0) - 180.0
    i_lat = np.where((lat_v >= C.GRID_SOUTH - 1e-6) & (lat_v <= C.GRID_NORTH + 1e-6))[0]
    i_lon = np.where((lon_180 >= C.GRID_WEST - 1e-6) & (lon_180 <= C.GRID_EAST + 1e-6))[0]
    obj = obj.isel({la: i_lat, lo: i_lon})
    obj = obj.assign_coords({lo: lon_180[i_lon]})

    if inicio is not None or fim is not None:
        obj = obj.sel({t: slice(inicio, fim)})

    nv = None
    for n in _NOMES_NIVEL:
        if n in obj.dims:
            nv = n
            break
    if nv is not None and niveis is not None:
        vals = np.round(obj[nv].values.astype(float)).astype(int)
        idx = []
        for alvo in niveis:
            hit = np.where(vals == alvo)[0]
            if len(hit):
                idx.append(int(hit[0]))
        obj = obj.isel({nv: idx})

    ren = {t: "valid_time", la: "latitude", lo: "longitude"}
    if nv is not None:
        ren[nv] = "pressure_level"
    obj = obj.rename({k: v for k, v in ren.items() if k != v})
    obj = obj.sortby("latitude", ascending=False).sortby("longitude")
    # coordenadas que nao sao dimensao (number, expver, step, surface...) saem
    extras = [c for c in obj.coords if c not in obj.dims]
    return obj.drop_vars(extras, errors="ignore")


# -------------------------------------------------------------------- validar
def validar(da: xr.DataArray, var: C.Var, ano: int | None, mes: int | None) -> list[str]:
    """Lista de problemas; vazia = arquivo bom. Nao levanta excecao."""
    problemas = []
    lat_ok, lon_ok = grade_esperada()
    for nome in ("latitude", "longitude"):
        if nome not in da.dims:
            return [f"sem dimensao {nome}"]
    if da.sizes["latitude"] != len(lat_ok) or da.sizes["longitude"] != len(lon_ok):
        problemas.append(f"grade {da.sizes['latitude']}x{da.sizes['longitude']}, "
                         f"esperado {len(lat_ok)}x{len(lon_ok)}")
    elif not (np.allclose(da.latitude.values, lat_ok, atol=1e-4)
              and np.allclose(da.longitude.values, lon_ok, atol=1e-4)):
        problemas.append(f"grade deslocada: lat {da.latitude.values[:2]}..., "
                         f"lon {da.longitude.values[:2]}...")

    if var.conjunto == "pressure" and not var.estatica:
        if "pressure_level" not in da.dims:
            problemas.append("sem dimensao pressure_level")
        else:
            veio = sorted(int(round(float(x))) for x in da.pressure_level.values)
            if veio != sorted(C.NIVEIS):
                problemas.append(f"niveis {veio}, esperado {sorted(C.NIVEIS)}")

    if ano is not None and not var.estatica:
        esperado = horas_do_mes(ano, mes)
        veio = pd.DatetimeIndex(da.valid_time.values) if "valid_time" in da.dims else None
        if veio is None:
            problemas.append("sem dimensao valid_time")
        elif len(veio) != len(esperado) or not (veio == esperado).all():
            faltam = len(esperado.difference(veio))
            problemas.append(f"{len(veio)} horas, esperado {len(esperado)} "
                             f"({faltam} faltando)")

    if da.size and bool(np.isnan(da.values).all()):
        problemas.append("campo inteiro NaN")
    return problemas


# --------------------------------------------------------------------- gravar
def gravar_atomico(da: xr.DataArray, destino: Path) -> None:
    """Grava em .part e renomeia: uma interrupcao nunca deixa um .nc pela
    metade que a proxima execucao confundiria com um arquivo pronto."""
    destino.parent.mkdir(parents=True, exist_ok=True)
    tmp = destino.with_suffix(destino.suffix + ".part")
    da.astype("float32").to_dataset().to_netcdf(tmp)
    tmp.replace(destino)


# ------------------------------------------------------------------- registro
class Registro:
    """Uma linha JSON por tentativa, em era5-bruto/_registro.jsonl.

    E onde fica dito, por variavel e mes, o que falhou e por que -- inclusive
    depois que o terminal fechou.
    """

    def __init__(self, raiz: Path):
        self.caminho = Path(raiz) / DIR_BRUTO / "_registro.jsonl"
        self.caminho.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def anotar(self, var: C.Var, ano, mes, fonte: str, status: str,
               motivo: str = "", segundos: float = 0.0) -> None:
        linha = {"quando": datetime.now().isoformat(timespec="seconds"),
                 "conjunto": var.conjunto, "var": var.curto, "ano": ano, "mes": mes,
                 "fonte": fonte, "status": status, "motivo": motivo[:500],
                 "segundos": round(segundos, 1)}
        with self._lock, open(self.caminho, "a") as f:
            f.write(json.dumps(linha, ensure_ascii=False) + "\n")

    def ultimas_falhas(self) -> dict:
        """(conjunto, var, ano, mes) -> motivo da ULTIMA tentativa, se ela falhou."""
        if not self.caminho.exists():
            return {}
        ultimo = {}
        for linha in self.caminho.read_text().splitlines():
            try:
                r = json.loads(linha)
            except json.JSONDecodeError:
                continue
            ultimo[(r["conjunto"], r["var"], r["ano"], r["mes"])] = r
        return {k: r["motivo"] for k, r in ultimo.items() if r["status"] == "falha"}


def esconder(texto: str, segredo: str | None) -> str:
    """Remove a chave de qualquer mensagem antes de imprimir ou registrar."""
    return texto.replace(segredo, "***") if segredo else texto


def agora() -> float:
    return time.monotonic()
