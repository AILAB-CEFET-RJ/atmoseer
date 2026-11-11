#!/usr/bin/env python
"""ETAPA 1 -- baixar. Cada (variavel, mes) vira UM arquivo em era5-bruto/.

O que baixar esta em era5_catalogo.py. Juntar por mes e a etapa 2
(juntar_era5.py), separada de proposito: aqui uma falha numa variavel nunca
derruba as outras. Se o `u` falhar em 2020-01, o terminal e o registro
(era5-bruto/_registro.jsonl) dizem "u 2020-01: FALHA <motivo>", e o resto
segue. Rodar de novo baixa so o que falta.

Fontes:
  EDH -- Earth Data Hub (DestinE), espelho oficial do ERA5 em Zarr, sem fila.
         Precisa de EDH_API_KEY no ambiente.
  CDS -- Copernicus, so para o que o EDH nao tem (totalx e vimdf).
         Precisa de ~/.cdsapirc.

As duas fontes podem rodar em PROCESSOS SEPARADOS (--fonte edh / --fonte cds):
o EDH termina em horas, a fila do CDS leva dias. (Variavel com fontes de
reserva, como ("edh", "cds"), so troca de fonte com --fonte todas.)

Uso (no env `era5`):
    python src/era5_gfs/baixar_era5.py --listar            # o que esta no catalogo
    python src/era5_gfs/baixar_era5.py --status            # o que ja baixou, falhou, falta
    python src/era5_gfs/baixar_era5.py --dry-run           # o que seria baixado
    python src/era5_gfs/baixar_era5.py                     # baixa tudo o que falta
    python src/era5_gfs/baixar_era5.py --anos 2020 --meses 1 --vars cape tp
"""
from __future__ import annotations

import argparse
import os
import shutil
import sys
import threading
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import xarray as xr

import era5_catalogo as C
import era5_comum as M


class VariavelAusente(Exception):
    """A fonte nao tem a variavel (ou a descartou). Unico erro que faz tentar a
    PROXIMA fonte do catalogo: erro de rede no EDH nao deve virar fila no CDS."""


class LimiteDeCusto(Exception):
    """CDS: 'cost limits exceeded -- your request is too large'."""


def log(msg: str) -> None:
    print(msg, flush=True)


# ============================================================= planejamento
def pendentes(raiz: Path, var: C.Var, meses: list[tuple[int, int]]) -> list[tuple[int, int]]:
    if var.estatica:
        return [] if M.caminho_estatico(raiz, var).exists() else [(None, None)]
    return [(a, m) for a, m in meses if not M.caminho_bruto(raiz, var, a, m).exists()]


def blocos_contiguos(meses: list[tuple[int, int]]) -> list[list[tuple[int, int]]]:
    """[(2017,9),(2017,10),...,(2018,5),(2018,9)] -> uma lista por sequencia de
    meses consecutivos. No EDH cada bloco e lido de uma vez: os chunks do Zarr
    cobrem 45-60 dias, e ler mes a mes baixaria o mesmo chunk duas vezes."""
    blocos = []
    for a, m in sorted(meses):
        if blocos:
            pa, pm = blocos[-1][-1]
            if (a, m) == ((pa, pm + 1) if pm < 12 else (pa + 1, 1)):
                blocos[-1].append((a, m))
                continue
        blocos.append([(a, m)])
    return blocos


def gravar_meses(raiz, reg, var, da, meses, fonte) -> int:
    """Parte um bloco em meses, valida cada um e grava os bons. Devolve n_ok."""
    ok = 0
    for a, m in meses:
        horas = M.horas_do_mes(a, m)
        parte = da.sel(valid_time=slice(horas[0], horas[-1]))
        problemas = M.validar(parte, var, a, m)
        rot = f"{var.conjunto:<8} {var.curto:<6} {a}-{m:02d}"
        if problemas:
            motivo = "; ".join(problemas)
            reg.anotar(var, a, m, fonte, "falha", motivo)
            log(f"  FALHA {rot}  [{fonte}] {motivo}")
            continue
        M.gravar_atomico(parte, M.caminho_bruto(raiz, var, a, m))
        reg.anotar(var, a, m, fonte, "ok")
        log(f"  ok    {rot}  [{fonte}]")
        ok += 1
    return ok


# ======================================================================= EDH
class FonteEDH:
    nome = "edh"

    def __init__(self, chave: str):
        self.chave = chave
        self._ds = {}
        self._lock = threading.Lock()

    def _abrir(self, conjunto: str) -> xr.Dataset:
        with self._lock:
            if conjunto not in self._ds:
                url = f"https://edh:{self.chave}@{C.EDH_ZARR[conjunto]}"
                try:
                    self._ds[conjunto] = xr.open_dataset(url, engine="zarr", chunks={})
                except Exception as exc:                      # noqa: BLE001
                    # a URL tem a chave; nenhuma mensagem sai sem esconde-la
                    raise RuntimeError(M.esconder(f"{type(exc).__name__}: {exc}", self.chave)) from None
            return self._ds[conjunto]

    def nome_na_fonte(self, var: C.Var) -> str:
        ds = self._abrir(var.conjunto)
        for n in var.candidatos_edh:
            if n in ds.data_vars:
                return n
        raise VariavelAusente(f"EDH nao tem {var.candidatos_edh}")

    def ler(self, var: C.Var, meses: list[tuple[int, int]]) -> xr.DataArray:
        da = self._abrir(var.conjunto)[self.nome_na_fonte(var)]
        niveis = C.NIVEIS if var.conjunto == "pressure" else None
        if var.estatica:
            t = next(n for n in ("valid_time", "time") if n in da.dims)
            da = M.recortar(da.isel({t: [0]}), niveis).isel(valid_time=0, drop=True)
            return da.load().rename(var.curto)
        inicio = M.horas_do_mes(*meses[0])[0]
        fim = M.horas_do_mes(*meses[-1])[-1]
        return M.recortar(da, niveis, inicio, fim).load().rename(var.curto)


def baixar_edh(raiz, reg, fonte: FonteEDH, vars_, meses, tentativas=3) -> list[C.Var]:
    """Baixa do EDH. Devolve as variaveis que o EDH NAO TEM (para o CDS)."""
    ausentes = []
    for var in vars_:
        falta = pendentes(raiz, var, meses)
        if not falta:
            continue
        if var.estatica:
            try:
                da = fonte.ler(var, [])
                problemas = M.validar(da, var, None, None)
                if problemas:
                    raise RuntimeError("; ".join(problemas))
                M.gravar_atomico(da, M.caminho_estatico(raiz, var))
                reg.anotar(var, None, None, "edh", "ok")
                log(f"  ok    {var.conjunto:<8} {var.curto:<6} estatico  [edh]")
            except VariavelAusente as exc:
                log(f"  --    {var.curto}: {exc}")
                ausentes.append(var)
            except Exception as exc:                          # noqa: BLE001
                motivo = M.esconder(f"{type(exc).__name__}: {exc}", fonte.chave)
                reg.anotar(var, None, None, "edh", "falha", motivo)
                log(f"  FALHA {var.conjunto:<8} {var.curto:<6} estatico  [edh] {motivo}")
            continue

        for bloco in blocos_contiguos(falta):
            rot = f"{var.conjunto} {var.curto} {bloco[0][0]}-{bloco[0][1]:02d}..{bloco[-1][0]}-{bloco[-1][1]:02d}"
            t0 = M.agora()
            for tentativa in range(1, tentativas + 1):
                try:
                    da = fonte.ler(var, bloco)
                    gravar_meses(raiz, reg, var, da, bloco, "edh")
                    break
                except VariavelAusente as exc:
                    log(f"  --    {var.curto}: {exc} -> fica para a proxima fonte")
                    if var not in ausentes:
                        ausentes.append(var)
                    break
                except Exception as exc:                      # noqa: BLE001
                    motivo = M.esconder(f"{type(exc).__name__}: {exc}", fonte.chave)
                    if tentativa < tentativas:
                        log(f"  ...   {rot}: {motivo[:160]} -- tentativa {tentativa}/{tentativas}, "
                            f"esperando {15 * tentativa}s")
                        time.sleep(15 * tentativa)
                        continue
                    for a, m in bloco:
                        reg.anotar(var, a, m, "edh", "falha", motivo, M.agora() - t0)
                    log(f"  FALHA {rot}  [edh] {motivo[:300]}")
            if var in ausentes:
                break
    return ausentes


# ======================================================================= CDS
_local = threading.local()


def cliente_cds():
    if not hasattr(_local, "c"):
        import cdsapi
        _local.c = cdsapi.Client(timeout=60, retry_max=5, sleep_max=60, quiet=True)
    return _local.c


def abrir_resposta(caminho: Path) -> xr.Dataset:
    """O CDS as vezes devolve zip (um membro por stepType). Junta os membros."""
    if not zipfile.is_zipfile(caminho):
        return xr.open_dataset(caminho).load()
    destino = caminho.parent / (caminho.stem + "_unzip")
    destino.mkdir(exist_ok=True)
    with zipfile.ZipFile(caminho) as z:
        z.extractall(destino)
    membros = sorted(destino.glob("*.nc"))
    if not membros:
        raise RuntimeError("zip do CDS sem .nc dentro")
    partes = [M.recortar(xr.open_dataset(f)) for f in membros]
    ds = xr.merge(partes, compat="override").load()
    shutil.rmtree(destino, ignore_errors=True)
    return ds


class FonteCDS:
    nome = "cds"
    # Um pedido = uma variavel e os meses de UM ano (ano x mes e produto
    # cartesiano, entao anos nunca se misturam). 1 variavel x 5 meses = 3.720
    # campos. Pedidos de 9 meses (6.696 campos) tambem foram aceitos e
    # concluidos em ~5 h (2026-10-06); 17.856 foi recusado com "cost limits
    # exceeded". Ficou 5 por escolha conservadora. Se um pedido for recusado
    # por tamanho, a recusa chega na hora e _pedido_cds divide ao meio sozinho.
    MAX_MESES = 5

    def __init__(self, raiz: Path):
        self.tmp = Path(raiz) / M.DIR_BRUTO / "_tmp_cds"
        self.tmp.mkdir(parents=True, exist_ok=True)

    def ler(self, var: C.Var, meses: list[tuple[int, int]]) -> xr.DataArray:
        ano = meses[0][0]
        assert all(a == ano for a, _ in meses), "um pedido ao CDS = um ano (ano x mes e produto cartesiano)"
        req = {
            "product_type": ["reanalysis"],
            "variable": [var.nome_cds],
            "year": [str(ano)],
            "month": [f"{m:02d}" for _, m in meses],
            "day": [f"{d:02d}" for d in range(1, 32)],
            "time": [f"{h:02d}:00" for h in range(24)],
            "data_format": "netcdf",
            "download_format": "zip",
            "area": [C.GRID_NORTH + 0.01, C.GRID_WEST - 0.01, C.GRID_SOUTH - 0.01, C.GRID_EAST + 0.01],
        }
        if var.conjunto == "pressure":
            req["pressure_level"] = [str(n) for n in C.NIVEIS]
        alvo = self.tmp / f"{var.conjunto}_{var.curto}_{ano}_{meses[0][1]}-{meses[-1][1]}_{threading.get_ident()}.zip"
        try:
            cliente_cds().retrieve(C.CDS_DATASET[var.conjunto], req, str(alvo))
        except Exception as exc:                              # noqa: BLE001
            msg = str(exc)
            if "cost limits exceeded" in msg or "too large" in msg.lower():
                raise LimiteDeCusto(msg.splitlines()[-1][:200]) from None
            raise
        try:
            ds = abrir_resposta(alvo)
        finally:
            alvo.unlink(missing_ok=True)
        if var.curto in ds.data_vars:
            da = ds[var.curto]
        elif len(ds.data_vars) == 1:
            da = ds[list(ds.data_vars)[0]]
        else:
            raise VariavelAusente(f"CDS devolveu {sorted(ds.data_vars)}, sem {var.curto}")
        return M.recortar(da, C.NIVEIS if var.conjunto == "pressure" else None).rename(var.curto)


def _pedido_cds(raiz, reg, fonte: FonteCDS, var, meses, tentativas=4) -> None:
    """Um pedido; se o CDS achar grande demais, divide os meses ao meio."""
    rot = f"{var.conjunto} {var.curto} {meses[0][0]}-{meses[0][1]:02d}..{meses[-1][1]:02d}"
    t0 = M.agora()
    for tentativa in range(1, tentativas + 1):
        try:
            log(f"  ->    {rot}  [cds] pedido enviado ({len(meses)} mes(es))")
            da = fonte.ler(var, meses)
            gravar_meses(raiz, reg, var, da, meses, "cds")
            return
        except LimiteDeCusto as exc:
            if len(meses) == 1:
                motivo = f"limite de custo mesmo com 1 mes: {exc}"
                break
            meio = len(meses) // 2
            log(f"  /2    {rot}: grande demais para o CDS, dividindo em {meio} + {len(meses) - meio}")
            _pedido_cds(raiz, reg, fonte, var, meses[:meio])
            _pedido_cds(raiz, reg, fonte, var, meses[meio:])
            return
        except VariavelAusente as exc:
            motivo = str(exc)
            break
        except Exception as exc:                              # noqa: BLE001
            motivo = f"{type(exc).__name__}: {exc}"
            recusa = any(k in motivo.lower() for k in ("reject", "limit", "too many", "quota"))
            espera = 30 * tentativa if recusa else 60 * 2 ** (tentativa - 1)
            if tentativa < tentativas:
                log(f"  ...   {rot}: {motivo[:160]} -- tentativa {tentativa}/{tentativas}, esperando {espera}s")
                time.sleep(espera)
    for a, m in meses:
        reg.anotar(var, a, m, "cds", "falha", motivo, M.agora() - t0)
    log(f"  FALHA {rot}  [cds] {motivo[:300]}")


def baixar_cds(raiz, reg, vars_, meses, workers: int) -> None:
    fonte = FonteCDS(raiz)
    pedidos = []
    for var in vars_:
        falta = pendentes(raiz, var, meses)
        por_ano = {}
        for a, m in falta:
            por_ano.setdefault(a, []).append((a, m))
        for lista in por_ano.values():
            for i in range(0, len(lista), FonteCDS.MAX_MESES):
                pedidos.append((var, lista[i:i + FonteCDS.MAX_MESES]))
    if not pedidos:
        return
    log(f"\n  CDS: {len(pedidos)} pedido(s), {workers} em paralelo. A fila do CDS leva horas "
        f"por pedido; o terminal fica quieto enquanto espera.")
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futuros = []
        for var, ms in pedidos:
            futuros.append(pool.submit(_pedido_cds, raiz, reg, fonte, var, ms))
            time.sleep(3)
        for f in as_completed(futuros):
            f.result()


# ==================================================================== status
def imprimir_status(raiz, vars_, meses) -> int:
    reg = M.Registro(raiz)
    falhas = reg.ultimas_falhas()
    n_ok = n_falha = n_pend = 0
    log("\nlegenda: # baixado   x falhou (ver motivo abaixo)   . pendente")
    for conjunto in ("pressure", "single"):
        vs = [v for v in vars_ if v.conjunto == conjunto]
        if not vs:
            continue
        log(f"\n{conjunto.upper()}" + " " * 9 + "  ".join(
            f"{bloco[0][0]}/{bloco[0][1]:02d}" for bloco in blocos_contiguos(meses)))
        for v in vs:
            if v.estatica:
                ok = M.caminho_estatico(raiz, v).exists()
                c = "#" if ok else ("x" if (v.conjunto, v.curto, None, None) in falhas else ".")
                log(f"  {v.curto:<8} {c} (estatico)")
                continue
            celulas = []
            for bloco in blocos_contiguos(meses):
                s = ""
                for a, m in bloco:
                    if M.caminho_bruto(raiz, v, a, m).exists():
                        s += "#"; n_ok += 1
                    elif (v.conjunto, v.curto, a, m) in falhas:
                        s += "x"; n_falha += 1
                    else:
                        s += "."; n_pend += 1
                celulas.append(s.ljust(7))
            log(f"  {v.curto:<8} " + " ".join(celulas))
    log(f"\n{n_ok} baixados, {n_falha} com falha, {n_pend} pendentes")
    # uma falha so esta "em aberto" se o arquivo ainda nao existe (uma nova
    # tentativa pode ter dado certo depois)
    por_nome = {(v.conjunto, v.curto): v for v in vars_}
    abertas = {}
    for (cj, curto, ano, mes), motivo in falhas.items():
        v = por_nome.get((cj, curto))
        if v is None:
            continue
        existe = (M.caminho_estatico(raiz, v) if v.estatica
                  else M.caminho_bruto(raiz, v, ano, mes)).exists()
        if not existe:
            abertas[(cj, curto, ano, mes)] = motivo
    if abertas:
        log("\nfalhas em aberto (ultima tentativa):")
        for (cj, v, a, m), motivo in sorted(abertas.items(), key=lambda x: str(x[0])):
            quando = "estatico" if a is None else f"{a}-{m:02d}"
            log(f"  {cj:<8} {v:<6} {quando}: {motivo[:200]}")
    return 0 if n_falha == 0 else 1


# ====================================================================== main
def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--listar", action="store_true", help="imprime o catalogo e sai")
    ap.add_argument("--status", action="store_true", help="o que ja baixou, falhou ou falta")
    ap.add_argument("--dry-run", action="store_true", help="mostra o que seria baixado")
    ap.add_argument("--conjunto", choices=["single", "pressure", "ambos"], default="ambos")
    ap.add_argument("--vars", nargs="+", default=None, metavar="CURTO", help="ex.: --vars t q cape")
    ap.add_argument("--fonte", choices=["edh", "cds", "todas"], default="todas")
    ap.add_argument("--anos", type=int, nargs="+", default=None)
    ap.add_argument("--meses", type=int, nargs="+", default=None)
    ap.add_argument("--workers", type=int, default=2, help="pedidos simultaneos ao CDS (default 2)")
    ap.add_argument("--raiz", type=Path, default=M.RAIZ_PADRAO)
    a = ap.parse_args(argv)

    if a.listar:
        log(C.tabela())
        return 0

    conjunto = None if a.conjunto == "ambos" else a.conjunto
    vars_ = C.selecionar(conjunto, a.vars)
    if not vars_:
        log(f"nenhuma variavel do catalogo bate com --vars {a.vars} --conjunto {a.conjunto}")
        return 2
    meses = C.periodo(a.anos, a.meses)

    if a.status:
        return imprimir_status(a.raiz, vars_, meses)

    usa_edh = a.fonte in ("edh", "todas")
    usa_cds = a.fonte in ("cds", "todas")
    vars_edh = [v for v in vars_ if v.fontes[0] == "edh"]
    vars_cds = [v for v in vars_ if v.fontes[0] == "cds"]

    log(f"periodo: {meses[0][0]}-{meses[0][1]:02d} .. {meses[-1][0]}-{meses[-1][1]:02d} ({len(meses)} meses)")
    for nome, vs, ativa in (("EDH", vars_edh, usa_edh), ("CDS", vars_cds, usa_cds)):
        pend = sum(len(pendentes(a.raiz, v, meses)) for v in vs)
        log(f"{nome}: {len(vs)} variaveis, {pend} arquivos (variavel x mes) faltando"
            + ("" if ativa else "  [desligada por --fonte]"))

    if a.dry_run:
        for v in vars_:
            falta = pendentes(a.raiz, v, meses)
            if falta:
                desc = "estatico" if v.estatica else ", ".join(
                    f"{b[0][0]}-{b[0][1]:02d}..{b[-1][0]}-{b[-1][1]:02d}" for b in blocos_contiguos(falta))
                log(f"  {v.conjunto:<8} {v.curto:<6} [{'+'.join(v.fontes)}] {desc}")
        return 0

    reg = M.Registro(a.raiz)
    if usa_edh and vars_edh:
        chave = os.environ.get("EDH_API_KEY", "").strip()
        if not chave:
            log("\nEDH_API_KEY nao definida. Gere a chave no Earth Data Hub e rode:\n"
                "    export EDH_API_KEY=...\n(as variaveis do CDS seguem mesmo assim)")
        else:
            log("\n=== EDH ===")
            ausentes = baixar_edh(a.raiz, reg, FonteEDH(chave), vars_edh, meses)
            for v in ausentes:
                if "cds" in v.fontes:
                    log(f"  {v.curto}: nao existe no EDH -> vai para o CDS")
                    vars_cds.append(v)
                else:
                    for am in pendentes(a.raiz, v, meses):
                        reg.anotar(v, am[0], am[1], "edh", "falha", "variavel ausente no EDH e sem CDS no catalogo")
    if usa_cds and vars_cds:
        log("\n=== CDS ===")
        baixar_cds(a.raiz, reg, vars_cds, meses, a.workers)

    log("\nresumo:")
    return imprimir_status(a.raiz, vars_, meses)


if __name__ == "__main__":
    sys.exit(main())
