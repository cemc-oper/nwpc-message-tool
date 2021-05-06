import typing
import os
import pathlib

import yaml


PACKAGE_NAME = "nwpc-message-tool"


def load_config(file_path: typing.Optional[str] = None):
    path = _get_config_path(file_path)
    with open(path) as f:
        config = yaml.safe_load(f)
        return config


def _get_config_path(file_path: typing.Optional[str] = None):
    if file_path is None:
        if "NWPC_OPER_CONFIG" in os.environ:
            return os.environ["NWPC_OPER_CONFIG"]
        path = pathlib.Path(pathlib.Path.home(), f".config/nwpc-oper/{PACKAGE_NAME}.yaml")
        return path
    else:
        return file_path
