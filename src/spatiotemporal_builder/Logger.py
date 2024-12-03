import io
import logging
from typing import cast

SUCCESS = logging.INFO - 5
logging.addLevelName(SUCCESS, "SUCCESS")


class MyLogger(logging.Logger):
    def success(self, msg, *args, **kwargs):
        if self.isEnabledFor(SUCCESS):
            self._log(SUCCESS, msg, args, **kwargs)


logging.setLoggerClass(MyLogger)


class ColorFormatter(logging.Formatter):
    error_color = "\033[91m"
    warning_color = "\033[93m"
    info_color = "\033[96m"
    success_color = "\033[92m"
    reset_color = "\033[0m"
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    FORMATS = {
        SUCCESS: f"{success_color}{log_format}{reset_color}",
        logging.DEBUG: f"{log_format}",
        logging.INFO: f"{info_color}{log_format}{reset_color}",
        logging.WARNING: f"{warning_color}{log_format}{reset_color}",
        logging.ERROR: f"{error_color}{log_format}{reset_color}",
    }

    def format(self, record):
        formatter = logging.Formatter(self.FORMATS.get(record.levelno), datefmt="%Y-%m-%d %H:%M:%S")
        return formatter.format(record)


class Logger:
    def __init__(self):
        self.logger = cast(MyLogger, logging.getLogger("log"))
        self.logger.setLevel(logging.DEBUG)

        stream_handler = logging.StreamHandler()
        file_handler = logging.FileHandler("websirenes_spatiotemporal_log.log")
        stream_handler.setFormatter(ColorFormatter())
        file_handler.setFormatter(
            logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
            )
        )
        self.logger.addHandler(stream_handler)
        self.logger.addHandler(file_handler)

    def get_logger(self, name: str) -> MyLogger:
        return self.logger.getChild(name)


# https://stackoverflow.com/questions/14897756/python-progress-bar-through-logging-module
class TqdmLogger(io.StringIO):
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.level = logging.DEBUG

    def write(self, buf: str) -> None:
        self.buf = buf.strip("\r\n\t ")

    def flush(self) -> None:
        self.logger.log(self.level, self.buf)


logger = Logger()
