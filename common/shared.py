"""Shared variables and data file api."""

import logging
try:  # Python 2.7+
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass
import os
import threading
import json

from config import CONFIG
from common import relmon


logger = logging.getLogger(__name__)
logger.addHandler(NullHandler())

lock = threading.RLock()
relmon_data = []
relmons = {}

if not os.path.isfile(CONFIG.DATA_FILE_NAME):
    with open(CONFIG.DATA_FILE_NAME, 'w') as new_file:
        new_file.write("[]")
else:
    with open(CONFIG.DATA_FILE_NAME) as json_file:
        relmon_data = json.load(json_file)
        for request_json in relmon_data:
            request = relmon.RelmonRequest(**request_json)
            relmons[request.id_] = request


def new(request):
    logger.info("Trying to insert new RelmonRequest " + str(request.id_))
    global relmons, controllers
    with lock:
        if (request.id_ in relmons.keys()):
            raise KeyError("RelmonRequest with given id_ already exists")
        relmons[request.id_] = request
        relmon_data.insert(0, request.to_dict())
        _write()
    logger.info("New RelmonRequest inserted")


def update(request_id):
    #Updating only statuses
    logger.info("Updating RelmonRequest " + str(request_id))
    with lock:
        for req_idx, request in enumerate(relmon_data):
            if (request["id_"] == request_id):
                relmon_data[req_idx] = relmons[request_id].to_dict()
                break
        _write()
    logger.info("RelmonRequest updated")

def updateEntireRequest(request_id, req_data):
    logger.info("Updating RelmonRequest " + str(request_id))
    global relmons
    with lock:
        for req_idx, request in enumerate(relmon_data):
            if (request["id_"] == request_id):
                tmp = req_data.to_dict()
                tmp['id_'] = request_id
                relmon_data[req_idx] = tmp
                relmons[request_id] = relmon.RelmonRequest(**tmp)
                break
        _write()
    logger.info("RelmonRequest updated")

def drop(request_id):
    logger.info("Dropping RelmonRequest " + str(request_id))
    with lock:
        relmons.pop(request_id)
        for req_idx, request in enumerate(relmon_data):
            if (request["id_"] == request_id):
                relmon_data.pop(req_idx)
                break
        _write()
    logger.info("RelmonRequest droped")


def _write():
    logger.info("Writing to data file")
    with open(CONFIG.DATA_FILE_NAME, 'w') as json_file:
        json_file.write(json.dumps(relmon_data, indent=4))
