"""Module providing tests for common Pythoid functionality as well as Spark extensions."""

import os
from pathlib import Path

LOG_MSG_FORMAT = "%(asctime)s %(levelname)s - %(name)s.%(funcName)s: %(message)s"
LOG_DATE_FORMAT = "%H:%M:%S"


def cur_dir() -> Path:
    """Returns current folder."""
    return Path(os.path.dirname(__file__))


def data_dir() -> Path:
    """Returns test data folder."""
    return Path(os.path.join(cur_dir(), "data"))


def data_filepath(name: str) -> Path:
    """Returns a path for a file in test data folder."""
    return Path(os.path.join(data_dir(), name))
