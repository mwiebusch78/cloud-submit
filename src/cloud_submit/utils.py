import os
import shutil


class CloudSubmitError(Exception):
    pass


def clear_path(path):
    shutil.rmtree(path, ignore_errors=True)
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


def ensure_path(path, clear=False):
    if clear:
        clear_path(path)
    os.makedirs(path, exist_ok=True)
