import json

from common_utils.logging import get_logger

logger = get_logger("config")


def _resolve(value, params):
    """Replace {placeholders} such as {catalog} inside every string of the config."""
    if isinstance(value, str):
        for key, replacement in params.items():
            value = value.replace("{" + key + "}", str(replacement))
        return value
    if isinstance(value, list):
        return [_resolve(item, params) for item in value]
    if isinstance(value, dict):
                return {_resolve(key, params): _resolve(item, params) for key, item in value.items()}
    return value


def load_config(config_path, **params):
    """
    Read a JSON config and fill its placeholders

    config_path : path to the json file (relative to the notebook)
    params      : values for placeholders, e.g. catalog="retaildataplatform"

    returns a dict
    """
    logger.info("Reading configuration from %s", config_path)

    with open(config_path, "r") as f:
        config = json.load(f)

    return _resolve(config, params)


def get_param(dbutils, name, default=""):
    """
    Create a notebook widget (if missing) and return its value.
    Job parameters with the same name override the default.
    """
    try:
        dbutils.widgets.text(name, default)
    except Exception:
        pass
    value = dbutils.widgets.get(name)
    return value if value != "" else default
