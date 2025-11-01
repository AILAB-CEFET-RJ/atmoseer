from pathlib import Path

from config import globals

DATA_ROOT = Path(globals.GOES16_DATA_DIR)
FEATURES_ROOT = Path(globals.GOES16_FEATURES_DIR)


def build_channel_paths_by_year(channel: str):
    """
    Retorna uma lista de caminhos para cada ano contendo dados do canal especificado.
    """
    temp = [
        DATA_ROOT / year.name / channel
        for year in sorted(DATA_ROOT.iterdir())
        if (DATA_ROOT / year.name / channel).is_dir()
    ]
    print(f"Found {len(temp)} directories for channel {channel} in {DATA_ROOT}")
    return temp


def build_feature_paths_by_year(feature: str):
    """
    Retorna uma lista de caminhos para cada ano contendo features já extraídas.
    """
    return [
        FEATURES_ROOT / feature / year.name
        for year in sorted((FEATURES_ROOT / feature).iterdir())
        if (FEATURES_ROOT / feature / year.name).is_dir()
    ]
