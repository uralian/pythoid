import os
from pathlib import Path


def cur_dir() -> Path:
    """Returns current folder."""
    return Path(os.path.dirname(__file__))


def data_dir() -> Path:
    """Returns test data folder."""
    return Path(os.path.join(cur_dir(), "data"))


def data_filepath(name: str) -> Path:
    """Returns a path for a file in test data folder."""
    return Path(os.path.join(data_dir(), name))
