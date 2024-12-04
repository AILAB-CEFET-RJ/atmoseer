import json
from argparse import Namespace

import pandas as pd

from .Logger import logger

log = logger.get_logger(__name__)


class Settings:
    def __init__(self) -> None:
        self.only_ERA5 = False
        self.start_date: pd.Timestamp = pd.Timestamp.min
        self.end_date: pd.Timestamp = pd.Timestamp.max
        self.ignored_months = []

    def set_settings(self, args: Namespace):
        for key, value in vars(args).items():
            if not hasattr(self, key):
                log.error(f"Unknown setting: {key}")
            setattr(self, key, value)
        log.info(f"Settings: {json.dumps((vars(self)), indent=4)}")


settings = Settings()
