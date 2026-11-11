"""Testes das partes do baixar_gfs.py que nao dependem de rede nem do Herbie."""
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import baixar_gfs as G      # noqa: E402
import era5_catalogo as C   # noqa: E402


def gfs(curto, conjunto):
    return next(v for v in G.CATALOGO if v.curto == curto and v.conjunto == conjunto)


# ------------------------------------------------------------ rodada e prazo
@pytest.mark.parametrize("hora,rodada,prazo", [
    ("2022-01-10 01:00", "2022-01-10 00:00", 1),
    ("2022-01-10 06:00", "2022-01-10 00:00", 6),
    ("2022-01-10 07:00", "2022-01-10 06:00", 1),
    ("2022-01-10 00:00", "2022-01-09 18:00", 6),
])
def test_hora_valida_vem_da_rodada_certa(hora, rodada, prazo):
    c0, lead = G.rodada_e_prazo(pd.Timestamp(hora))
    assert (c0, lead) == (pd.Timestamp(rodada), prazo)


def test_reserva_usa_a_rodada_anterior_com_prazo_mais_6():
    c0, lead = G.rodada_e_prazo(pd.Timestamp("2022-01-10 07:00"), reserva=True)
    assert (c0, lead) == (pd.Timestamp("2022-01-10 00:00"), 7)


def test_todas_as_horas_do_mes_tem_prazo_1_a_6():
    for h in pd.date_range("2022-01-01", periods=744, freq="h"):
        c0, lead = G.rodada_e_prazo(h)
        assert 1 <= lead <= 6 and c0.hour in (0, 6, 12, 18) and c0 + pd.Timedelta(hours=lead) == h


# ----------------------------------------------------- acumulados e medias
def test_acumulado_do_balde_vira_hora_isolada():
    chuva_por_hora = np.array([0.0, 2.0, 0.5, 0.0, 3.0, 1.0])     # prazos 1..6
    acum = np.cumsum(chuva_por_hora)                               # como o GFS publica: 0-L
    for lead in range(1, 7):
        ant = acum[lead - 2] if lead > 1 else None
        assert G.hora_de_acumulado(acum[lead - 1], ant, lead) == pytest.approx(chuva_por_hora[lead - 1])


def test_balde_reinicia_no_prazo_7():
    assert G.inicio_balde(6) == 0 and G.inicio_balde(7) == 6 and G.inicio_balde(12) == 6
    # prazo 7 publica "6-7 hour acc": ja e a hora isolada
    assert G.hora_de_acumulado(np.array(4.0), np.array(99.0), 7) == 4.0


def test_media_do_balde_vira_media_da_hora():
    fluxo = np.array([100.0, 300.0, 200.0, 0.0, 50.0, 400.0])
    medias = np.cumsum(fluxo) / np.arange(1, 7)                    # "0-L hour ave"
    for lead in range(1, 7):
        ant = medias[lead - 2] if lead > 1 else None
        assert G.hora_de_media(medias[lead - 1], ant, lead) == pytest.approx(fluxo[lead - 1])


def test_acumulado_negativo_por_arredondamento_vira_zero():
    assert G.hora_de_acumulado(np.array(1.0), np.array(1.0000001), 3) == 0.0


# ----------------------------------------------------------------- derivadas
def test_ponto_de_orvalho_no_ar_saturado_e_a_temperatura():
    # ar saturado a 20 C e 1000 hPa: q ~ 0,01468 kg/kg
    assert G.ponto_de_orvalho_c(0.01468, 1000.0) == pytest.approx(20.0, abs=0.2)


def test_indices_k_e_total_totals():
    # exemplo de livro: T850 20, T700 8, T500 -10, Td850 16, Td700 2
    assert G.indice_k(20, 8, -10, 16, 2) == pytest.approx(40.0)
    assert G.total_totals(20, -10, 16) == pytest.approx(56.0)


def test_coriolis_negativo_no_hemisferio_sul():
    f = G.coriolis(-22.75)
    assert f < 0 and f == pytest.approx(-5.64e-5, rel=1e-2)


# ------------------------------------------------------- buscas no .idx
LINHAS_IDX = pd.Series([
    ":TMP:850 mb:1 hour fcst", ":TMP:85 mb:1 hour fcst", ":TMP:850 mb:11 hour fcst",
    ":TMP:2 m above ground:1 hour fcst",
    ":PWAT:entire atmosphere (considered as a single layer):1 hour fcst",
    ":APCP:surface:0-1 hour acc fcst", ":APCP:surface:0-3 hour acc fcst",
    ":APCP:surface:6-8 hour acc fcst",
    ":TCDC:entire atmosphere:0-2 hour ave fcst",
    ":CIN:surface:1 hour fcst", ":CIN:180-0 mb above ground:1 hour fcst",
])


def bate(busca):
    return list(LINHAS_IDX[LINHAS_IDX.str.contains(busca, regex=True)])


def test_busca_de_nivel_de_pressao_e_estrita():
    assert bate(G.busca_no_prazo(gfs("t", "pressure"), 1)) == [":TMP:850 mb:1 hour fcst"]


def test_busca_do_pwat_acha_o_nivel_longo():
    assert len(bate(G.busca_no_prazo(gfs("tcwv", "single"), 1))) == 1


def test_busca_de_acumulado_respeita_o_balde():
    assert bate(G.busca_no_prazo(gfs("tp", "single"), 1)) == [":APCP:surface:0-1 hour acc fcst"]
    assert bate(G.busca_no_prazo(gfs("tp", "single"), 8)) == [":APCP:surface:6-8 hour acc fcst"]


def test_busca_de_media_e_de_superficie_nao_confundem():
    assert bate(G.busca_no_prazo(gfs("tcc", "single"), 2)) == [":TCDC:entire atmosphere:0-2 hour ave fcst"]
    assert bate(G.busca_no_prazo(gfs("cin", "single"), 1)) == [":CIN:surface:1 hour fcst"]


# ------------------------------------------------------------ compatibilidade
def test_toda_variavel_do_era5_esta_no_catalogo_gfs():
    era5 = {(v.conjunto, v.curto) for v in C.VARIAVEIS}
    gfs_ = {(v.conjunto, v.curto) for v in G.CATALOGO}
    assert era5 == gfs_


def test_periodo_comeca_no_arquivo_do_aws():
    p = G.periodo(None, None)
    assert p[0] >= G.INICIO_ARQUIVO and p[-1] == C.ULTIMO_MES
