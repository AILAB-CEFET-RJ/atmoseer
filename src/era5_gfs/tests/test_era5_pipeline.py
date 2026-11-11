"""Testes do pipeline de download em duas etapas (baixar_era5 / juntar_era5).

Nenhum teste toca a rede. O EDH falso troca so a ABERTURA do Zarr: leitura,
recorte, validacao e gravacao sao o codigo real.

    conda run -n era5 python -m pytest scripts/tests -q
"""
import json
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import baixar_era5 as B     # noqa: E402
import era5_catalogo as C   # noqa: E402
import era5_comum as M      # noqa: E402

CHAVE = "chave-secreta-123"


def var(conjunto, curto):
    return next(v for v in C.VARIAVEIS if v.conjunto == conjunto and v.curto == curto and not v.estatica)


def ds_falso(nomes_pressure=(), nomes_single=(), inicio="2020-01-01", fim="2020-01-31T23:00"):
    """Dataset 'como o EDH': regiao MAIOR que o dominio, longitude 0..360,
    latitude norte->sul, niveis em ordem crescente e com niveis a mais."""
    t = pd.date_range(inicio, fim, freq="h")
    lat = np.arange(-20.0, -26.01, -0.25)
    lon = np.arange(312.0, 320.01, 0.25)
    niveis = [100, 200, 300, 500, 700, 850, 925, 1000]
    rng = np.random.default_rng(0)
    dv = {}
    for n in nomes_pressure:
        dv[n] = (("valid_time", "pressure_level", "latitude", "longitude"),
                 rng.random((len(t), len(niveis), len(lat), len(lon)), dtype=np.float32))
    for n in nomes_single:
        dv[n] = (("valid_time", "latitude", "longitude"),
                 rng.random((len(t), len(lat), len(lon)), dtype=np.float32))
    return xr.Dataset(dv, coords=dict(valid_time=t, latitude=lat, longitude=lon,
                                      pressure_level=niveis, number=0))


class EDHFalso(B.FonteEDH):
    def __init__(self, dss, falhar=()):
        super().__init__(CHAVE)
        self._ds = dss
        self.falhar = set(falhar)
        self.chamadas = []

    def ler(self, var, meses):
        self.chamadas.append((var.conjunto, var.curto))
        if var.curto in self.falhar:
            raise RuntimeError(f"falha simulada no {var.curto}")
        return super().ler(var, meses)


@pytest.fixture
def raiz(tmp_path):
    return tmp_path


JAN = [(2020, 1)]


# ------------------------------------------------------------------ etapa 1
def test_falha_no_u_nao_derruba_os_outros(raiz, monkeypatch):
    monkeypatch.setattr(B.time, "sleep", lambda s: None)
    fonte = EDHFalso({"pressure": ds_falso(["t", "u"]), "single": ds_falso([], ["tp"])},
                     falhar={"u"})
    vs = [var("pressure", "t"), var("pressure", "u"), var("single", "tp")]
    B.baixar_edh(raiz, M.Registro(raiz), fonte, vs, JAN)

    assert M.caminho_bruto(raiz, vs[0], 2020, 1).exists()        # t
    assert not M.caminho_bruto(raiz, vs[1], 2020, 1).exists()    # u
    assert M.caminho_bruto(raiz, vs[2], 2020, 1).exists()        # tp
    falhas = M.Registro(raiz).ultimas_falhas()
    assert list(falhas) == [("pressure", "u", 2020, 1)]
    assert "falha simulada no u" in falhas[("pressure", "u", 2020, 1)]


def test_rodar_de_novo_so_pede_o_que_falta(raiz, monkeypatch):
    monkeypatch.setattr(B.time, "sleep", lambda s: None)
    dss = {"pressure": ds_falso(["t", "u"]), "single": ds_falso([], ["tp"])}
    vs = [var("pressure", "t"), var("pressure", "u"), var("single", "tp")]
    B.baixar_edh(raiz, M.Registro(raiz), EDHFalso(dss, falhar={"u"}), vs, JAN)
    segunda = EDHFalso(dss)
    B.baixar_edh(raiz, M.Registro(raiz), segunda, vs, JAN)
    assert segunda.chamadas == [("pressure", "u")]
    assert M.Registro(raiz).ultimas_falhas() == {}


def test_arquivo_bruto_tem_grade_niveis_e_nomes_padronizados(raiz):
    v = var("pressure", "t")
    B.baixar_edh(raiz, M.Registro(raiz), EDHFalso({"pressure": ds_falso(["t"])}), [v], JAN)
    with xr.open_dataset(M.caminho_bruto(raiz, v, 2020, 1)) as ds:
        lat, lon = M.grade_esperada()
        assert list(ds.t.dims) == ["valid_time", "pressure_level", "latitude", "longitude"]
        np.testing.assert_allclose(ds.latitude.values, lat)          # norte -> sul
        np.testing.assert_allclose(ds.longitude.values, lon)         # -180..180
        assert ds.pressure_level.values.tolist() == C.NIVEIS         # so os 6, na ordem
        assert ds.sizes["valid_time"] == 744
        assert "number" not in ds.coords


def test_bloco_da_estacao_e_lido_de_uma_vez_e_partido_em_meses(raiz):
    v = var("single", "tp")
    meses = [(2019, 12), (2020, 1), (2020, 2)]
    fonte = EDHFalso({"single": ds_falso([], ["tp"], "2019-12-01", "2020-02-29T23:00")})
    B.baixar_edh(raiz, M.Registro(raiz), fonte, [v], meses)
    assert fonte.chamadas == [("single", "tp")]                      # uma leitura
    for a, m in meses:
        assert M.caminho_bruto(raiz, v, a, m).exists()


def test_mes_com_horas_faltando_nao_e_gravado(raiz):
    v = var("single", "tp")
    fonte = EDHFalso({"single": ds_falso([], ["tp"], fim="2020-01-30T23:00")})   # falta o dia 31
    B.baixar_edh(raiz, M.Registro(raiz), fonte, [v], JAN)
    assert not M.caminho_bruto(raiz, v, 2020, 1).exists()
    assert "horas" in M.Registro(raiz).ultimas_falhas()[("single", "tp", 2020, 1)]


# variavel hipotetica com nome alternativo no EDH e CDS de reserva -- o
# mecanismo existe no catalogo mesmo que nenhuma variavel real o use hoje
COM_RESERVA = C.Var("vimdf", "vertical_integral_of_divergence_of_moisture_flux",
                    "single", "dinamica", ("edh", "cds"), nomes_edh=("vimdf", "viwvd"))


def test_nome_alternativo_no_edh_vira_o_nome_do_catalogo(raiz):
    v = COM_RESERVA
    B.baixar_edh(raiz, M.Registro(raiz), EDHFalso({"single": ds_falso([], ["viwvd"])}), [v], JAN)
    with xr.open_dataset(M.caminho_bruto(raiz, v, 2020, 1)) as ds:
        assert list(ds.data_vars) == ["vimdf"]


def test_variavel_ausente_no_edh_fica_para_o_cds(raiz):
    v = COM_RESERVA
    ausentes = B.baixar_edh(raiz, M.Registro(raiz), EDHFalso({"single": ds_falso([], ["tp"])}), [v], JAN)
    assert ausentes == [v] and "cds" in v.fontes


def test_vimd_acumulado_nao_e_aceito_no_lugar_do_vimdf(raiz):
    """paramId 213 (vimd, acumulado, kg m-2) e outra variavel."""
    v = COM_RESERVA
    ausentes = B.baixar_edh(raiz, M.Registro(raiz), EDHFalso({"single": ds_falso([], ["vimd"])}), [v], JAN)
    assert ausentes == [v]


def test_estatica_baixa_um_timestep(raiz):
    v = next(x for x in C.VARIAVEIS if x.estatica)
    B.baixar_edh(raiz, M.Registro(raiz), EDHFalso({"single": ds_falso([], ["z"])}), [v], JAN)
    with xr.open_dataset(M.caminho_estatico(raiz, v)) as ds:
        assert "valid_time" not in ds.dims and ds.z.shape == (9, 11)


def test_chave_nunca_vai_para_o_registro(raiz, monkeypatch):
    monkeypatch.setattr(B.time, "sleep", lambda s: None)

    def abre_e_falha(url, **kw):
        raise OSError(f"nao consegui abrir {url}")
    monkeypatch.setattr(B.xr, "open_dataset", abre_e_falha)
    B.baixar_edh(raiz, M.Registro(raiz), B.FonteEDH(CHAVE), [var("single", "tp")], JAN)
    texto = (raiz / M.DIR_BRUTO / "_registro.jsonl").read_text()
    assert CHAVE not in texto and "***" in texto


def test_cds_divide_o_pedido_quando_e_grande_demais(raiz, monkeypatch):
    monkeypatch.setattr(B.time, "sleep", lambda s: None)
    tamanhos = []

    def ler(self, v, meses):
        tamanhos.append(len(meses))
        if len(meses) > 2:
            raise B.LimiteDeCusto("cost limits exceeded")
        ini = M.horas_do_mes(*meses[0])[0]
        fim = M.horas_do_mes(*meses[-1])[-1]
        return M.recortar(ds_falso([], ["totalx"], ini, fim)["totalx"])

    monkeypatch.setattr(B.FonteCDS, "ler", ler)
    v = var("single", "totalx")
    meses = [(2020, m) for m in (1, 2, 3, 4, 5)]
    B.baixar_cds(raiz, M.Registro(raiz), [v], meses, workers=1)
    # 5 recusado -> 2 + 3; o 3 tambem e recusado -> 1 + 2
    assert tamanhos == [5, 2, 3, 1, 2]
    assert all(M.caminho_bruto(raiz, v, a, m).exists() for a, m in meses)
    assert M.Registro(raiz).ultimas_falhas() == {}


def test_pedido_ao_cds_nunca_mistura_anos(raiz, monkeypatch):
    """ano x mes no CDS e produto cartesiano: dez/2019 + jan/2020 num pedido so
    baixaria tambem jan/2019 e dez/2020."""
    pedidos = []
    monkeypatch.setattr(B, "_pedido_cds", lambda raiz, reg, f, v, ms: pedidos.append(ms))
    B.baixar_cds(raiz, M.Registro(raiz), [var("single", "totalx")],
                 [(2019, 11), (2019, 12), (2020, 1), (2020, 2)], workers=1)
    assert all(len({a for a, _ in ms}) == 1 for ms in pedidos)


# ------------------------------------------------------------------ etapa 2
def _baixar(raiz, vs, falhar=()):
    dss = {"pressure": ds_falso([v.curto for v in vs if v.conjunto == "pressure"]),
           "single": ds_falso([], [v.curto for v in vs if v.conjunto == "single"])}
    B.baixar_edh(raiz, M.Registro(raiz), EDHFalso(dss, falhar), vs, JAN)


def test_juntar_nao_grava_mes_incompleto_e_diz_o_que_falta(raiz, monkeypatch):
    import juntar_era5 as J
    monkeypatch.setattr(B.time, "sleep", lambda s: None)
    vs = [var("pressure", "t"), var("pressure", "u")]
    _baixar(raiz, vs, falhar={"u"})
    situacao, problemas = J.juntar_mes(raiz, "pressure", 2020, 1, vs, parcial=False, refazer=False)
    assert situacao == "incompleto" and problemas == ["u: falta"]
    assert not M.caminho_final(raiz, "pressure", 2020, 1).exists()
    situacao, _ = J.juntar_mes(raiz, "pressure", 2020, 1, vs, parcial=True, refazer=False)
    assert situacao == "gravado"


def test_juntar_completo_e_idempotente(raiz):
    import juntar_era5 as J
    vs = [var("pressure", "t"), var("pressure", "q")]
    _baixar(raiz, vs)
    assert J.juntar_mes(raiz, "pressure", 2020, 1, vs, False, False)[0] == "gravado"
    assert J.juntar_mes(raiz, "pressure", 2020, 1, vs, False, False)[0] == "atualizado"
    with xr.open_dataset(M.caminho_final(raiz, "pressure", 2020, 1)) as ds:
        assert set(ds.data_vars) == {"t", "q"}
        assert set(ds.dims) == {"valid_time", "pressure_level", "latitude", "longitude"}


def test_juntar_pega_arquivo_bruto_corrompido_sem_abortar(raiz):
    import juntar_era5 as J
    vs = [var("pressure", "t"), var("pressure", "q")]
    _baixar(raiz, vs)
    M.caminho_bruto(raiz, vs[1], 2020, 1).write_bytes(b"lixo")
    situacao, problemas = J.juntar_mes(raiz, "pressure", 2020, 1, vs, False, False)
    assert situacao == "incompleto" and problemas[0].startswith("q: ilegivel")


def test_registro_e_jsonl_legivel(raiz, monkeypatch):
    monkeypatch.setattr(B.time, "sleep", lambda s: None)
    _baixar(raiz, [var("pressure", "t"), var("pressure", "u")], falhar={"u"})
    linhas = [json.loads(l) for l in (raiz / M.DIR_BRUTO / "_registro.jsonl").read_text().splitlines()]
    assert {(l["var"], l["status"]) for l in linhas} == {("t", "ok"), ("u", "falha")}
