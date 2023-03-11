from configparser import ConfigParser


def get_config(config_file: str) -> ConfigParser:
    """Create config parser for reading config file"""
    config = ConfigParser()
    config.read(config_file)

    return config
