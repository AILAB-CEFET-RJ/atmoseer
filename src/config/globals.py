import os


def _get_env(key: str, default: str) -> str:
    return os.getenv(key, default)


def _get_env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, default))
    except (TypeError, ValueError):
        return default

INMET_API_BASE_URL = _get_env("INMET_API_BASE_URL", "https://apitempo.inmet.gov.br")

# Weather stations datasource directories
WS_INMET_DATA_DIR = _get_env("WS_INMET_DATA_DIR", "./data/ws/inmet/")
WS_ALERTARIO_DATA_DIR = _get_env("WS_ALERTARIO_DATA_DIR", "./data/ws/alertario/ws/")
GS_ALERTARIO_DATA_DIR = _get_env("GS_ALERTARIO_DATA_DIR", "./data/ws/alertario/rain_gauge_era5_fused/")
CEMADEN_DATA_DIR = _get_env("CEMADEN_DATA_DIR", ".data/ws/cemaden/")

SUMARE_DATA_DIR = _get_env("SUMARE_DATA_DIR", ".data/radar_sumare")
# WS_GOES_DATA_DIR = "atmoseer/data/ws/goes16"
GOES16_DATA_DIR = _get_env("GOES16_DATA_DIR", "./data/goes16/CMI/")

# Directory to store the extracted features from GOES-16 data
GOES16_FEATURES_DIR = _get_env("GOES16_FEATURES_DIR", "./data/goes16/features/")

# Atmospheric sounding datasource directory
NWP_DATA_DIR = _get_env("NWP_DATA_DIR", "./data/NWP/")

# Atmospheric sounding datasource directory
AS_DATA_DIR = _get_env("AS_DATA_DIR", "./data/as/")

# GOES16/DSI directory
DSI_DATA_DIR = _get_env("DSI_DATA_DIR", "./data/goes16/DSI")

# GOES16/TPW directory
TPW_DATA_DIR = _get_env("TPW_DATA_DIR", "./data/goes16/wsoi")

# Directory to store the train/val/test datasets for each weather station of interest
DATASETS_DIR = _get_env("DATASETS_DIR", "./data/datasets/")

# Directory to store the generated models and their corresponding reports
MODELS_DIR = _get_env("MODELS_DIR", "./models/")

# see https://portal.inmet.gov.br/paginas/catalogoaut
INMET_WEATHER_STATION_IDS = (
    "A601",  # Seropédica
    "A602",  # Marambaia
    "A621",  # Vila militar
    "A627",  # Niteroi
    "A636",  # Jacarepagua
    "A652",  # Forte de Copacabana
)

ALERTARIO_GAUGE_STATION_IDS = (
    "anchieta",
    "av_brasil_mendanha",
    "bangu",
    "barrinha",
    "campo_grande",
    "cidade_de_deus",
    "copacabana",
    "grajau_jacarepagua",
    "grajau",
    "grande_meier",
    "grota_funda",
    "ilha_do_governador",
    "laranjeiras",
    "madureira",
    "penha",
    "piedade",
    "recreio",
    "rocinha",
    "santa_teresa",
    "saude",
    "sepetiba",
    "tanque",
    "tijuca_muda",
    "tijuca",
    "urca",
    "alto_da_boa_vista",  # **
    "iraja",  # **
    "jardim_botanico",  # **
    "riocentro",  # **
    "santa_cruz",  # **
    "vidigal",  # **
)

ALERTARIO_WEATHER_STATION_IDS = (
    "guaratiba",  # **
    "sao_cristovao",  # **
)

WEBSIRENE_DATA_DIR = _get_env("WEBSIRENE_DATA_DIR", "./data/ws/websirene/")

WEBSIRENE_STATION_IDS = (
    "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", 
    "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
    "21", "22", "23", "24", "25", "26", "27", "28", "29", "30",
    "31", "32", "33", "34", "35", "36", "37", "38", "39", "40",
    "41", "42", "43", "44", "45", "46", "47", "48", "49", "50",
    "51", "52", "53", "54", "55", "56", "57", "58", "59", "60",
    "61", "62", "63", "64", "65", "66", "67", "68", "69", "70",
    "71", "72", "73", "74", "75", "76", "77", "78", "79", "80",
    "81", "82", "83" 
)

# lower left corner
lat_min = _get_env_float("REGION_LAT_MIN", -23.801876626302175)
lon_min = _get_env_float("REGION_LON_MIN", -45.05290312102409)

# upper right corner
lat_max = _get_env_float("REGION_LAT_MAX", -21.699774257353113)
lon_max = _get_env_float("REGION_LON_MAX", -42.35676996062447)

# Region of Interest
extent = [lon_min, lat_min, lon_max, lat_max]

