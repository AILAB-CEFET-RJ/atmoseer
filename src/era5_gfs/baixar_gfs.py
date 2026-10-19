#!/usr/bin/env python
"""Baixa o GFS 0,25 grau, de hora em hora, no MESMO formato do ERA5.

Saida: gfs-bruto/<conjunto>/<var>/RJ_<ano>_<mes>.nc -- mesma grade 9 x 11,
mesmos nomes curtos e MESMAS UNIDADES do era5-bruto/. Assim um modelo treinado
com ERA5 pode ser avaliado com GFS, que e o dado que existiria em tempo real.

Por que GFS (e nao HRRR, ICON, IFS aberto...):
  * HRRR so cobre os EUA continentais;
  * GFS 0,25 grau usa exatamente os pontos da grade do ERA5 e tem saida HORARIA
    nas primeiras 120 h -- e o unico modelo global aberto com niveis de pressao
    nessa resolucao e frequencia;
  * o arquivo no AWS (fonte padrao do Herbie) comeca em ~fev/2021, o que cobre
    as estacoes de TESTE do split cronologico (2020/21 a 2025/26).

Como a serie horaria e montada
------------------------------
GFS e previsao, nao reanalise. Para cada hora V usamos a rodada C das 00/06/12/18
UTC mais recente e o prazo L = 1..6 h (V = C + L): horas 01-06 vem da rodada das
00, 07-12 da das 06, e assim por diante. E a mesma construcao do "GFS analysis"
do dynamical.org. Se uma rodada faltar, a hora vem da rodada anterior com prazo
L + 6 (7..12 h).

Campos acumulados (tp, cp) e medias (ssr, tcc) no GFS valem desde o inicio do
balde de 6 h; a hora isolada sai da diferenca entre prazos consecutivos.

Compatibilidade com o ERA5 (ver CATALOGO abaixo)
------------------------------------------------
  direto    : t q r z u v w clwc ciwc crwc cswc o3 cape tcwv d2m t2m u10 v10
              sp msl blh tp cp ssr tcc tclw, z estatico
  derivado  : vo  = vorticidade absoluta - f (Coriolis)
              kx, totalx = calculados de t e q em 850/700/500 hPa
  ausente   : d, pv, cc, tciw, vimdf -- o GFS nao publica
  atencao   : cin vem NEGATIVO no GFS e e trocado de sinal; o GFS usa 0 onde o
              ERA5 deixa NaN, entao o canal `cin_def` do notebook nao tem o
              mesmo significado com GFS.
              clwc/ciwc/crwc/cswc no GFS sao razao de mistura, no ERA5 conteudo
              especifico -- diferenca < 1 % nesses valores.
              tclw no GFS (CWAT) e agua de nuvem TOTAL (liquida + gelo).

Volume: cada campo do GFS e GLOBAL e nao da para recortar no servidor. Baixamos
so as mensagens necessarias (byte-ranges pelo .idx), mas ainda sao dezenas de MB
por hora. Rode --estimar antes para ver o total.

Dependencias (no env `era5`):
    pip install herbie-data          # traz cfgrib; o eccodes vem como wheel
Uso:
    python src/era5_gfs/baixar_gfs.py --listar
    python src/era5_gfs/baixar_gfs.py --estimar           # MB por hora e total (so le .idx)
    python src/era5_gfs/baixar_gfs.py --dry-run
    python src/era5_gfs/baixar_gfs.py --anos 2022 --meses 1 --workers 4
    python src/era5_gfs/baixar_gfs.py --status
"""
from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
import threading
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

import era5_catalogo as C
import era5_comum as M

G = 9.80665              # m s-2: altura geopotencial (gpm) -> geopotencial (m2 s-2)
OMEGA = 7.2921e-5        # s-1: rotacao da Terra, para f = 2 Omega sin(lat)
DIR_GFS = "gfs-bruto"
FONTES = ["aws", "google", "azure", "nomads"]
INICIO_ARQUIVO = (2021, 3)   # primeiro mes completo do GFS 0,25 no AWS

NIV = "(" + "|".join(str(n) for n in C.NIVEIS) + ") mb"


@dataclass(frozen=True)
class VarGFS:
    curto: str               # nome ERA5 -- o mesmo do era5_catalogo
    conjunto: str            # "single" ou "pressure"
    tipo: str                # inst | acum | media | ssr | vo | kx | totalx | estatica | ausente
    busca: str = ""          # regex do Herbie sobre o .idx (sem o prazo)
    fator: float = 1.0       # multiplica para chegar na unidade do ERA5
    nota: str = ""


CATALOGO = [
    # ================= PRESSURE (C.NIVEIS) =================
    VarGFS("t",    "pressure", "inst", f":TMP:{NIV}:"),
    VarGFS("q",    "pressure", "inst", f":SPFH:{NIV}:"),
    VarGFS("r",    "pressure", "inst", f":RH:{NIV}:"),
    VarGFS("z",    "pressure", "inst", f":HGT:{NIV}:", G, "altura geopotencial (gpm) x g"),
    VarGFS("u",    "pressure", "inst", f":UGRD:{NIV}:"),
    VarGFS("v",    "pressure", "inst", f":VGRD:{NIV}:"),
    VarGFS("d",    "pressure", "ausente", nota="GFS nao publica divergencia"),
    VarGFS("w",    "pressure", "inst", f":VVEL:{NIV}:"),
    VarGFS("vo",   "pressure", "vo",   f":ABSV:{NIV}:", nota="vorticidade absoluta - f"),
    VarGFS("pv",   "pressure", "ausente", nota="GFS nao publica PV em niveis de pressao"),
    VarGFS("cc",   "pressure", "ausente", nota="GFS nao publica cobertura por nivel"),
    VarGFS("clwc", "pressure", "inst", f":CLMR:{NIV}:", nota="razao de mistura"),
    VarGFS("ciwc", "pressure", "inst", f":ICMR:{NIV}:", nota="razao de mistura"),
    VarGFS("crwc", "pressure", "inst", f":RWMR:{NIV}:", nota="razao de mistura"),
    VarGFS("cswc", "pressure", "inst", f":SNMR:{NIV}:", nota="razao de mistura"),
    VarGFS("o3",   "pressure", "inst", f":O3MR:{NIV}:"),
    # ================= SINGLE =================
    VarGFS("cape",   "single", "inst", ":CAPE:surface:"),
    VarGFS("cin",    "single", "inst", ":CIN:surface:", -1.0, "GFS negativo -> trocado de sinal"),
    VarGFS("kx",     "single", "kx",   nota="calculado de t e q em 850/700/500"),
    VarGFS("totalx", "single", "totalx", nota="calculado de t e q em 850/500"),
    VarGFS("tcwv",   "single", "inst", ":PWAT:entire atmosphere[^:]*:"),
    VarGFS("d2m",    "single", "inst", ":DPT:2 m above ground:"),
    VarGFS("t2m",    "single", "inst", ":TMP:2 m above ground:"),
    VarGFS("vimdf",  "single", "ausente", nota="GFS nao publica"),
    VarGFS("u10",    "single", "inst", ":UGRD:10 m above ground:"),
    VarGFS("v10",    "single", "inst", ":VGRD:10 m above ground:"),
    VarGFS("tcc",    "single", "media", ":TCDC:entire atmosphere[^:]*:", 0.01, "% -> fracao"),
    VarGFS("tclw",   "single", "inst", ":CWAT:entire atmosphere[^:]*:", nota="liquida + gelo"),
    VarGFS("tciw",   "single", "ausente", nota="GFS nao separa gelo na coluna"),
    VarGFS("sp",     "single", "inst", ":PRES:surface:"),
    VarGFS("msl",    "single", "inst", ":PRMSL:mean sea level:"),
    VarGFS("blh",    "single", "inst", ":HPBL:surface:"),
    VarGFS("cp",     "single", "acum", ":ACPCP:surface:", 1e-3, "mm -> m"),
    VarGFS("ssr",    "single", "ssr",  nota="(DSWRF - USWRF) medio na hora x 3600 -> J m-2"),
    VarGFS("tp",     "single", "acum", ":APCP:surface:", 1e-3, "mm -> m"),
    VarGFS("z",      "single", "estatica", ":HGT:surface:", G, "orografia (gpm) x g"),
]


# =========================================================== tempo e prazos
def rodada_e_prazo(v: pd.Timestamp, reserva: bool = False) -> tuple[pd.Timestamp, int]:
    """Hora valida -> (rodada, prazo). Normal: prazo 1..6; reserva: 7..12."""
    lead = (v.hour - 1) % 6 + 1
    if reserva:
        lead += 6
    return v - pd.Timedelta(hours=lead), lead


def inicio_balde(lead: int) -> int:
    """Os acumulados/medias do GFS reiniciam a cada 6 h: prazo 1..6 -> 0, 7..12 -> 6."""
    return ((lead - 1) // 6) * 6


def busca_no_prazo(var: VarGFS, lead: int, busca: str | None = None) -> str:
    busca = busca or var.busca
    b = inicio_balde(lead)
    if var.tipo == "acum":
        return f"{busca}{b}-{lead} hour acc fcst"
    if var.tipo in ("media", "ssr"):
        return f"{busca}{b}-{lead} hour ave fcst"
    return f"{busca}{lead} hour fcst"


def hora_de_acumulado(atual, anterior, lead: int):
    """Acumulado desde o inicio do balde -> so a ultima hora (nunca negativo)."""
    if anterior is None or lead - 1 == inicio_balde(lead):
        return np.clip(atual, 0, None)
    return np.clip(atual - anterior, 0, None)


def hora_de_media(atual, anterior, lead: int):
    """Media desde o inicio do balde -> media so da ultima hora."""
    b = inicio_balde(lead)
    if anterior is None or lead - 1 == b:
        return atual
    return (lead - b) * atual - (lead - 1 - b) * anterior


# ================================================================ derivadas
def ponto_de_orvalho_c(q, p_hpa):
    """Td (graus C) a partir de umidade especifica (kg/kg) e pressao (hPa)."""
    e = np.maximum(q * p_hpa / (0.622 + 0.378 * q), 1e-6)
    ln = np.log(e / 6.112)
    return 243.5 * ln / (17.67 - ln)


def indice_k(t850, t700, t500, td850, td700):
    """K = (T850 - T500) + Td850 - (T700 - Td700), em graus C."""
    return (t850 - t500) + td850 - (t700 - td700)


def total_totals(t850, t500, td850):
    """TT = T850 + Td850 - 2 T500, em graus C."""
    return t850 + td850 - 2 * t500


def coriolis(lat):
    return 2 * OMEGA * np.sin(np.deg2rad(lat))


# ============================================================ leitura GFS
def _campo(h, busca: str, nivel: bool):
    """Uma busca no .idx -> DataArray recortado no dominio (niveis em C.NIVEIS)."""
    ds = h.xarray(busca, remove_grib=True)
    if isinstance(ds, list):
        raise RuntimeError(f"busca ambigua ({len(ds)} grupos): {busca}")
    da = ds[list(ds.data_vars)[0]]
    t = pd.Timestamp(da.valid_time.values)
    da = da.expand_dims(valid_time=[t])
    da = M.recortar(da, C.NIVEIS if nivel else None)
    return da.isel(valid_time=0, drop=True)


def ler_rodada(rodada: str, leads: list[int], vars_: list[str], tmp: str) -> dict:
    """Roda num PROCESSO separado (eccodes nao e thread-safe).

    Devolve {(curto, conjunto, hora_valida_iso): ndarray} e {curto: erro} para
    o que falhou. Uma variavel que falha nao impede as outras.
    """
    from herbie import Herbie

    cat = {(v.curto, v.conjunto): v for v in CATALOGO}
    pedidas = [cat[k] for k in (tuple(x.split("|")) for x in vars_)]
    c0 = pd.Timestamp(rodada)
    saida, erros = {}, {}
    precisa = sorted(set(leads) | {l - 1 for l in leads
                                   if any(v.tipo in ("acum", "media", "ssr") for v in pedidas)
                                   and l - 1 > inicio_balde(l)})
    hs = {}
    for lead in precisa:
        h = Herbie(c0, model="gfs", product="pgrb2.0p25", fxx=lead, priority=FONTES,
                   save_dir=tmp, verbose=False)
        if h.grib is None:
            erros["*"] = f"rodada {c0:%Y-%m-%d %HZ} prazo {lead} nao encontrada"
            return {"campos": saida, "erros": erros}
        hs[lead] = h

    for v in pedidas:
        for lead in leads:
            hv = (c0 + pd.Timedelta(hours=lead)).isoformat()
            try:
                if v.tipo == "inst":
                    val = _campo(hs[lead], busca_no_prazo(v, lead), v.conjunto == "pressure").values
                elif v.tipo == "vo":
                    da = _campo(hs[lead], busca_no_prazo(v, lead), True)
                    val = (da - coriolis(da.latitude)).transpose(*da.dims).values
                elif v.tipo in ("kx", "totalx"):
                    t = _campo(hs[lead], busca_no_prazo(VarGFS("t", "pressure", "inst", f":TMP:{NIV}:"), lead), True)
                    q = _campo(hs[lead], busca_no_prazo(VarGFS("q", "pressure", "inst", f":SPFH:{NIV}:"), lead), True)
                    tc = {n: t.sel(pressure_level=n).values - 273.15 for n in (850, 700, 500)}
                    td = {n: ponto_de_orvalho_c(q.sel(pressure_level=n).values, n) for n in (850, 700)}
                    val = (indice_k(tc[850], tc[700], tc[500], td[850], td[700]) if v.tipo == "kx"
                           else total_totals(tc[850], tc[500], td[850]))
                elif v.tipo in ("acum", "media"):
                    atual = _campo(hs[lead], busca_no_prazo(v, lead), False).values
                    ant = (_campo(hs[lead - 1], busca_no_prazo(v, lead - 1), False).values
                           if lead - 1 > inicio_balde(lead) else None)
                    val = (hora_de_acumulado if v.tipo == "acum" else hora_de_media)(atual, ant, lead)
                elif v.tipo == "ssr":
                    liq = []
                    for l in (lead, lead - 1):
                        if l <= inicio_balde(lead):
                            liq.append(None)
                            continue
                        dn = _campo(hs[l], busca_no_prazo(v, l, ":DSWRF:surface:"), False).values
                        up = _campo(hs[l], busca_no_prazo(v, l, ":USWRF:surface:"), False).values
                        liq.append(dn - up)
                    val = hora_de_media(liq[0], liq[1], lead) * 3600.0
                else:
                    continue
                saida[(v.curto, v.conjunto, hv)] = (np.asarray(val, dtype="float32") * v.fator)
            except Exception as exc:                                      # noqa: BLE001
                erros[f"{v.conjunto}|{v.curto}"] = f"{type(exc).__name__}: {exc}"[:300]
    return {"campos": saida, "erros": erros}


# ===================================================================== mes
def baixar_mes(raiz: Path, reg, ano: int, mes: int, vars_: list[VarGFS], workers: int) -> None:
    horas = M.horas_do_mes(ano, mes)
    pendentes = [v for v in vars_ if not caminho(raiz, v, ano, mes).exists()]
    if not pendentes:
        return
    chaves = [f"{v.conjunto}|{v.curto}" for v in pendentes]
    lat, lon = M.grade_esperada()
    campos, erros_mes = {}, {}

    def tarefas(reserva: bool, so_horas=None):
        por_rodada = {}
        for hv in (so_horas if so_horas is not None else horas):
            c0, lead = rodada_e_prazo(hv, reserva)
            por_rodada.setdefault(c0.isoformat(), []).append(lead)
        return por_rodada

    tmp = tempfile.mkdtemp(prefix="gfs_", dir=str(raiz / DIR_GFS))
    try:
        for reserva in (False, True):
            faltando = [hv for hv in horas if any((v.curto, v.conjunto, hv.isoformat()) not in campos
                                                  for v in pendentes)]
            if not faltando:
                break
            por_rodada = tarefas(reserva, faltando)
            with ProcessPoolExecutor(max_workers=workers) as pool:
                fut = {pool.submit(ler_rodada, r, sorted(set(ls)), chaves, tmp): r
                       for r, ls in por_rodada.items()}
                for f in as_completed(fut):
                    try:
                        res = f.result()
                    except Exception as exc:                              # noqa: BLE001
                        erros_mes[fut[f]] = f"{type(exc).__name__}: {exc}"[:300]
                        continue
                    campos.update(res["campos"])
                    for k, e in res["erros"].items():
                        erros_mes[f"{fut[f]} {k}"] = e
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    for v in pendentes:
        var_era5 = C.selecionar(v.conjunto, [v.curto], incluir_estaticas=False)[0]
        rot = f"{v.conjunto:<8} {v.curto:<6} {ano}-{mes:02d}"
        faltam = [hv for hv in horas if (v.curto, v.conjunto, hv.isoformat()) not in campos]
        if faltam:
            exemplos = [e for k, e in erros_mes.items()
                        if k.endswith(f"{v.conjunto}|{v.curto}") or k.endswith("*")][:2]
            motivo = f"{len(faltam)} de {len(horas)} horas faltando. " + " | ".join(exemplos)
            reg.anotar(var_era5, ano, mes, "gfs", "falha", motivo)
            print(f"  FALHA {rot}  [gfs] {motivo[:200]}", flush=True)
            continue
        dados = np.stack([campos[(v.curto, v.conjunto, hv.isoformat())] for hv in horas])
        if v.conjunto == "pressure":
            da = xr.DataArray(dados, name=v.curto,
                              dims=("valid_time", "pressure_level", "latitude", "longitude"),
                              coords=dict(valid_time=horas, pressure_level=C.NIVEIS, latitude=lat, longitude=lon))
        else:
            da = xr.DataArray(dados, name=v.curto, dims=("valid_time", "latitude", "longitude"),
                              coords=dict(valid_time=horas, latitude=lat, longitude=lon))
        problemas = M.validar(da, var_era5, ano, mes)
        if problemas:
            reg.anotar(var_era5, ano, mes, "gfs", "falha", "; ".join(problemas))
            print(f"  FALHA {rot}  [gfs] {'; '.join(problemas)}", flush=True)
            continue
        M.gravar_atomico(da, caminho(raiz, v, ano, mes))
        reg.anotar(var_era5, ano, mes, "gfs", "ok")
        print(f"  ok    {rot}  [gfs]", flush=True)


def baixar_estatica(raiz: Path, reg, v: VarGFS) -> None:
    from herbie import Herbie
    destino = raiz / DIR_GFS / v.conjunto / v.curto / "estatico.nc"
    if destino.exists():
        return
    var_era5 = next(x for x in C.VARIAVEIS if x.estatica)
    try:
        h = Herbie("2022-01-01 00:00", model="gfs", product="pgrb2.0p25", fxx=1,
                   priority=FONTES, save_dir=str(raiz / DIR_GFS), verbose=False)
        da = (_campo(h, busca_no_prazo(v, 1), False) * v.fator).astype("float32").rename(v.curto)
        M.gravar_atomico(da, destino)
        reg.anotar(var_era5, None, None, "gfs", "ok")
        print(f"  ok    single   z      estatico  [gfs]", flush=True)
    except Exception as exc:                                              # noqa: BLE001
        reg.anotar(var_era5, None, None, "gfs", "falha", f"{type(exc).__name__}: {exc}")
        print(f"  FALHA single   z      estatico  [gfs] {exc}", flush=True)


# ================================================================== suporte
def caminho(raiz: Path, v: VarGFS, ano: int, mes: int) -> Path:
    return Path(raiz) / DIR_GFS / v.conjunto / v.curto / f"RJ_{ano}_{mes}.nc"


class RegistroGFS(M.Registro):
    def __init__(self, raiz: Path):
        self.caminho = Path(raiz) / DIR_GFS / "_registro.jsonl"
        self.caminho.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()


def periodo(anos, meses) -> list[tuple[int, int]]:
    return [am for am in C.periodo(anos, meses) if am >= INICIO_ARQUIVO]


def listar() -> None:
    print(f"GFS 0,25 grau, pgrb2.0p25, prazos 1-6 h de cada rodada (00/06/12/18 UTC)\n")
    for conjunto in ("pressure", "single"):
        print(conjunto.upper())
        for v in CATALOGO:
            if v.conjunto != conjunto:
                continue
            fonte = v.busca.strip(":") if v.busca else ""
            print(f"  {v.curto:<7} {v.tipo:<9} {fonte:<34} {v.nota}")
    n_ok = sum(v.tipo != "ausente" for v in CATALOGO)
    print(f"\n{n_ok} de {len(CATALOGO)} variaveis do catalogo ERA5 tem equivalente no GFS")


def estimar(anos, meses) -> None:
    """So le o .idx de uma rodada e soma o tamanho das mensagens necessarias."""
    from herbie import Herbie
    total = 0
    for lead in range(1, 7):
        h = Herbie("2022-01-15 00:00", model="gfs", product="pgrb2.0p25", fxx=lead,
                   priority=FONTES, verbose=False)
        idx = h.inventory()
        buscas = []
        for v in CATALOGO:
            if v.tipo in ("inst", "vo", "acum", "media"):
                buscas.append(busca_no_prazo(v, lead))
            elif v.tipo == "ssr":
                buscas += [busca_no_prazo(v, lead, ":DSWRF:surface:"),
                           busca_no_prazo(v, lead, ":USWRF:surface:")]
        sel = idx[idx.search_this.str.contains("|".join(buscas), regex=True)]
        total += int((sel.end_byte - sel.start_byte).sum())
    por_hora = total / 6
    n_horas = sum(len(M.horas_do_mes(a, m)) for a, m in periodo(anos, meses))
    print(f"~{por_hora / 2**20:.0f} MB por hora (todas as variaveis)")
    print(f"{n_horas} horas no periodo -> ~{por_hora * n_horas / 2**30:.0f} GB transferidos")
    print("(no disco ficam so os recortes 9 x 11: algumas centenas de MB)")


def status(raiz: Path, meses_, vars_) -> None:
    for v in vars_:
        feitos = sum(caminho(raiz, v, a, m).exists() for a, m in meses_)
        print(f"  {v.conjunto:<8} {v.curto:<7} {feitos:>3}/{len(meses_)}")
    falhas = RegistroGFS(raiz).ultimas_falhas()
    if falhas:
        print(f"\n{len(falhas)} falha(s) na ultima tentativa (ver gfs-bruto/_registro.jsonl)")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--listar", action="store_true")
    ap.add_argument("--estimar", action="store_true", help="volume a transferir (so le um .idx)")
    ap.add_argument("--status", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--anos", type=int, nargs="+", default=None)
    ap.add_argument("--meses", type=int, nargs="+", default=None)
    ap.add_argument("--vars", nargs="+", default=None, metavar="CURTO")
    ap.add_argument("--workers", type=int, default=4, help="rodadas em paralelo (processos)")
    ap.add_argument("--raiz", type=Path, default=M.RAIZ_PADRAO)
    a = ap.parse_args(argv)

    if a.listar:
        listar()
        return 0
    meses_ = periodo(a.anos, a.meses)
    vars_ = [v for v in CATALOGO if v.tipo not in ("ausente", "estatica")
             and (not a.vars or v.curto in a.vars)]
    if a.estimar:
        estimar(a.anos, a.meses)
        return 0
    if a.status:
        status(a.raiz, meses_, vars_)
        return 0

    pend = [(an, m) for an, m in meses_ if any(not caminho(a.raiz, v, an, m).exists() for v in vars_)]
    print(f"GFS: {len(vars_)} variaveis, {len(meses_)} meses no periodo "
          f"({meses_[0][0]}-{meses_[0][1]:02d} .. {meses_[-1][0]}-{meses_[-1][1]:02d}), {len(pend)} com algo faltando")
    if a.dry_run:
        return 0

    (a.raiz / DIR_GFS).mkdir(parents=True, exist_ok=True)
    reg = RegistroGFS(a.raiz)
    if not a.vars or "z" in a.vars:
        baixar_estatica(a.raiz, reg, next(v for v in CATALOGO if v.tipo == "estatica"))
    for ano, mes in pend:
        print(f"\n=== {ano}-{mes:02d} ===", flush=True)
        baixar_mes(a.raiz, reg, ano, mes, vars_, a.workers)
    return 0


if __name__ == "__main__":
    sys.exit(main())
