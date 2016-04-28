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
    logger.info("Initializing controller")
    controller.Controller()
