import os

import yaml

"""
To parse yaml config as dictionary
"""

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
config_file = os.path.join(ROOT_DIR, "config.yaml")
with open(config_file, "r") as f:
    config = yaml.load(f, Loader=yaml.FullLoader)
