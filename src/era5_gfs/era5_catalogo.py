"""O QUE baixamos do ERA5. So dados -- nenhuma logica de download mora aqui.

Para acrescentar uma variavel: uma linha em VARIAVEIS. Para tirar: comentar a
linha. Os dois scripts (baixar_era5.py e juntar_era5.py) leem daqui.

    python src/era5_gfs/baixar_era5.py --listar      # imprime esta tabela agrupada
"""
from __future__ import annotations

from dataclasses import dataclass

# ---------------------------------------------------------------------------
# Dominio: grade NATIVA do ERA5, 9 lat x 11 lon a 0,25 grau.
# (O dataset antigo usava uma grade interpolada e deslocada; esta e a nativa.)
# ---------------------------------------------------------------------------
GRID_NORTH, GRID_SOUTH = -21.75, -23.75        # 9 pontos
GRID_WEST, GRID_EAST = -45.00, -42.50          # 11 pontos
RESOLUTION = 0.25

# ---------------------------------------------------------------------------
# Periodo: estacao chuvosa (set -> mai), 1997 a 2026.
# Mudar ANOS e rodar de novo: o que ja existe e pulado.
# ULTIMO_MES corta o fim: o EDH vai ate 15/09/2026, entao o ultimo mes
# COMPLETO da estacao e mai/2026. Mes incompleto seria gravado como falha.
# ---------------------------------------------------------------------------
ANOS = list(range(1997, 2027))
MESES = [9, 10, 11, 12, 1, 2, 3, 4, 5]
ULTIMO_MES = (2026, 5)

# ---------------------------------------------------------------------------
# Niveis de pressao (hPa). 1000 fica abaixo do relevo em 57 dos 99 pixels e
# 925 em 20 (medido em 2020-01); o notebook gera o canal `acima_solo` a partir
# de `sp` para o modelo saber onde o valor e extrapolado.
# ---------------------------------------------------------------------------
NIVEIS = [1000, 925, 850, 700, 500, 200]

# Fontes. EDH = Earth Data Hub (DestinE): espelho oficial do ERA5 em Zarr,
# sem fila. CDS = Copernicus Climate Data Store: so para o que o EDH nao tem.
EDH_ZARR = {
    "single": "data.earthdatahub.destine.eu/era5/era5-single-levels-atmosphere-v0.zarr",
    "pressure": "data.earthdatahub.destine.eu/era5/era5-pressure-levels-v0.zarr",
}
CDS_DATASET = {
    "single": "reanalysis-era5-single-levels",
    "pressure": "reanalysis-era5-pressure-levels",
}


@dataclass(frozen=True)
class Var:
    curto: str              # nome no arquivo final (e no notebook)
    nome_cds: str           # nome longo usado no pedido ao CDS
    conjunto: str           # "single" ou "pressure"
    grupo: str              # grupo fisico, para leitura e para a ablacao
    fontes: tuple           # ordem de tentativa: ("edh",), ("cds",), ("edh", "cds")
    nomes_edh: tuple = ()   # nomes aceitos no EDH; vazio = o proprio `curto`
    estatica: bool = False  # constante no tempo: baixa 1 timestep so

    @property
    def candidatos_edh(self) -> tuple:
        return self.nomes_edh or (self.curto,)


VARIAVEIS = [
    # ===================== PRESSURE LEVELS (todos os NIVEIS) ==================
    # termodinamica
    Var("t",    "temperature",                          "pressure", "termodinamica", ("edh",)),
    Var("q",    "specific_humidity",                    "pressure", "termodinamica", ("edh",)),
    Var("r",    "relative_humidity",                    "pressure", "termodinamica", ("edh",)),
    # estrutura / estado
    Var("z",    "geopotential",                         "pressure", "estrutura",     ("edh",)),
    # dinamica
    Var("u",    "u_component_of_wind",                  "pressure", "dinamica",      ("edh",)),
    Var("v",    "v_component_of_wind",                  "pressure", "dinamica",      ("edh",)),
    Var("d",    "divergence",                           "pressure", "dinamica",      ("edh",)),
    Var("w",    "vertical_velocity",                    "pressure", "dinamica",      ("edh",)),
    Var("vo",   "vorticity",                            "pressure", "dinamica",      ("edh",)),
    Var("pv",   "potential_vorticity",                  "pressure", "dinamica",      ("edh",)),
    # nuvens / hidrometeoros
    Var("cc",   "fraction_of_cloud_cover",              "pressure", "hidrometeoros", ("edh",)),
    Var("clwc", "specific_cloud_liquid_water_content",  "pressure", "hidrometeoros", ("edh",)),
    Var("ciwc", "specific_cloud_ice_water_content",     "pressure", "hidrometeoros", ("edh",)),
    Var("crwc", "specific_rain_water_content",          "pressure", "hidrometeoros", ("edh",)),
    Var("cswc", "specific_snow_water_content",          "pressure", "hidrometeoros", ("edh",)),
    # quimica
    Var("o3",   "ozone_mass_mixing_ratio",              "pressure", "quimica",       ("edh",)),

    # ===================== SINGLE LEVEL =======================================
    # instabilidade
    Var("cape",   "convective_available_potential_energy", "single", "instabilidade", ("edh",)),
    Var("cin",    "convective_inhibition",                 "single", "instabilidade", ("edh",)),
    Var("kx",     "k_index",                               "single", "instabilidade", ("edh",)),
    # totalx NAO existe no EDH. No CDS, so chega quando pedido SOZINHO:
    # misturado com cape/cin/tcwv ele foi descartado sem aviso (2026-09-23).
    Var("totalx", "total_totals_index",                    "single", "instabilidade", ("cds",)),
    # termodinamica
    Var("tcwv",   "total_column_water_vapour",             "single", "termodinamica", ("edh",)),
    Var("d2m",    "2m_dewpoint_temperature",               "single", "termodinamica", ("edh",)),
    Var("t2m",    "2m_temperature",                        "single", "termodinamica", ("edh",)),
    # dinamica. vimdf = paramId 162084, kg m-2 s-1, INSTANTANEO (confirmado no
    # ecCodes). NAO aceitar `vimd` (paramId 213): e outra variavel, acumulada,
    # em kg m-2. CONFIRMADO em 2026-10-05: o EDH nao tem nem `vimdf` nem
    # `viwvd`, entao ela vem sempre do CDS (um mes = um pedido pequeno).
    Var("vimdf",  "vertical_integral_of_divergence_of_moisture_flux",
                                                           "single", "dinamica",      ("cds",)),
    Var("u10",    "10m_u_component_of_wind",               "single", "dinamica",      ("edh",)),
    Var("v10",    "10m_v_component_of_wind",               "single", "dinamica",      ("edh",)),
    # nuvens
    Var("tcc",    "total_cloud_cover",                     "single", "nuvens",        ("edh",)),
    Var("tclw",   "total_column_cloud_liquid_water",       "single", "nuvens",        ("edh",)),
    Var("tciw",   "total_column_cloud_ice_water",          "single", "nuvens",        ("edh",)),
    # estado da superficie
    Var("sp",     "surface_pressure",                      "single", "estado",        ("edh",)),
    Var("msl",    "mean_sea_level_pressure",               "single", "estado",        ("edh",)),
    Var("blh",    "boundary_layer_height",                 "single", "estado",        ("edh",)),
    # precipitacao e radiacao (acumuladas na hora que termina em valid_time)
    Var("cp",     "convective_precipitation",              "single", "precipitacao",  ("edh",)),
    Var("ssr",    "surface_net_solar_radiation",           "single", "precipitacao",  ("edh",)),
    Var("tp",     "total_precipitation",                   "single", "alvo",          ("edh",)),
    # estatica: orografia (geopotencial de superficie). Vai para era5-estatico/,
    # fora dos diretorios que o notebook le com *.nc.
    Var("z",      "geopotential",                          "single", "estatico",      ("edh",),
        estatica=True),
]


def selecionar(conjunto: str | None = None, nomes: list[str] | None = None,
               incluir_estaticas: bool = True) -> list[Var]:
    out = []
    for v in VARIAVEIS:
        if conjunto and v.conjunto != conjunto:
            continue
        if nomes and v.curto not in nomes:
            continue
        if v.estatica and not incluir_estaticas:
            continue
        out.append(v)
    return out


def periodo(anos=None, meses=None) -> list[tuple[int, int]]:
    """(ano, mes) em ordem cronologica."""
    anos = anos or ANOS
    meses = meses or MESES
    return sorted((a, m) for a in anos for m in meses if (a, m) <= ULTIMO_MES)


def tabela() -> str:
    linhas = []
    for conjunto in ("pressure", "single"):
        titulo = (f"PRESSURE LEVELS  ({', '.join(map(str, NIVEIS))} hPa)"
                  if conjunto == "pressure" else "SINGLE LEVEL")
        linhas.append(f"\n{titulo}")
        grupos = {}
        for v in selecionar(conjunto):
            grupos.setdefault(v.grupo, []).append(v)
        for grupo, vs in grupos.items():
            itens = ", ".join(
                v.curto + ("" if v.fontes == ("edh",) else f" [{'+'.join(v.fontes).upper()}]")
                + (" (estatica)" if v.estatica else "")
                for v in vs)
            linhas.append(f"  {grupo:<15} {itens}")
    n_p = len(selecionar("pressure"))
    n_s = len(selecionar("single"))
    linhas.append(f"\n{n_p} variaveis de pressure x {len(NIVEIS)} niveis, {n_s} de single level")
    ini, fim = periodo()[0], periodo()[-1]
    linhas.append(f"Periodo: estacao {MESES[0]:02d}..{MESES[-1]:02d}, de {ini[0]}-{ini[1]:02d} a "
                  f"{fim[0]}-{fim[1]:02d} ({len(periodo())} meses)   Fonte padrao: EDH; [CDS] = so CDS")
    return "\n".join(linhas)
