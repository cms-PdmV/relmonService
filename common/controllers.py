"""Shared 'controllers' variable"""

import logging

try:  # Python 2.7+
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

import controller
from common import shared

logger = logging.getLogger(__name__)
logger.addHandler(NullHandler())
controllers = {}


def init_controllers():
    logger.info("Initializing controllers")
    controller.Controller()
    # for relmon_request in shared.relmons.itervalues():
    #     logger.debug("Init controller" + str(relmon_request.id_))
    #     controllers[relmon_request.id_] = controller.Controller(relmon_request)
    #     # controllers[relmon_request.id_].start()
    #     logger.debug("Controller " + str(relmon_request.id_) + " started")
