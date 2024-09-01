import yaml
from utils.logger import logger

try:
    with open("config.yml", "r") as yml:
        cfg = yaml.load(yml, Loader=yaml.FullLoader)

    files = cfg['files']
except Exception as err:
    logger.error("%s, Error: %s", str(__name__), str(err))

