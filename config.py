import ConfigParser
import json
import logging
try:  # Python 2.7+
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass


logger = logging.getLogger(__name__)
logger.addHandler(NullHandler())


class UpdatableStruct():
    def _update(self, **entries):
        self.__dict__.update(entries)

conf_dict = {}
cfg_parser = ConfigParser.RawConfigParser()
logger.info("Reading configuration file 'config'")
cfg_parser.read("config")
for item in cfg_parser.items("strings"):
    conf_dict[item[0].upper()] = item[1]
for item in cfg_parser.items("lists"):
    conf_dict[item[0].upper()] = json.loads(item[1])
for option in cfg_parser.options("ints"):
    conf_dict[option.upper()] = cfg_parser.getint("ints", option)
for option in cfg_parser.options("booleans"):
    conf_dict[option.upper()] = cfg_parser.getboolean("booleans", option)

CONFIG = UpdatableStruct()
CONFIG._update(**conf_dict)


def setstring(name, value):
    logger.info("Updating configuration file")
    logger.info(name + "=" + value)
    cfg_parser.set("strings", name, value)
    CONFIG._update(name=value)


def write():
    logger.info("Writing to configuration file")
    with open("config", 'w') as cfgfile:
        cfg_parser.write(cfgfile)
