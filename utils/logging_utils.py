"""
Log configuration setup recipe taken from:

    https://fangpenlin.com/posts/2012/08/26/good-logging-practice-in-python/

"""
import logging
from logging import config as log_conf

import simplejson
import yaml

log = logging.getLogger(__name__)


def __load_yml_config(config_file):
    with open(config_file, "r") as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            log.error(exc)
            raise exc


def init_logger(config_path: str) -> None:
    """
    Setup logging configuration
    :param path: default path (absolute or relative)

    Args:
        config_path:
    """
    '''
    with open(config_path) as config_file:
        try:
            config_dict = __load_yml_config(config_file)
            log_conf.dictConfig(config_dict)
        except simplejson.errors.JSONDecodeError as error:
            raise IOError(f"Cannot read log config from path='{config_path}'")
    '''
    try:
        config_dict = __load_yml_config(config_path)
        log_conf.dictConfig(config_dict)
    except simplejson.errors.JSONDecodeError as error:
        raise IOError(f"Cannot read log config from path='{config_path}'")

