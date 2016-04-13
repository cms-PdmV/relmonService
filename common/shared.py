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
import time

from config import CONFIG
import common


logger = logging.getLogger(__name__)
logger.addHandler(NullHandler())

lock = threading.RLock()
relmons = {}

if not os.path.isfile(CONFIG.DATA_FILE_NAME):
    with open(CONFIG.DATA_FILE_NAME, 'w') as new_file:
        new_file.write("[]")
else:
    with open(CONFIG.DATA_FILE_NAME) as json_file:
        for request_json in json.load(json_file):
            request = common.relmon.RelmonRequest(**request_json)
            relmons[request.id_] = request


def new(request):
    logger.info("Trying to insert new RelmonRequest " + str(request.id_))
    with lock:
        if (request.id_ in relmons):
            raise KeyError("RelmonRequest with given id_ already exists")
        relmons[request.id_] = request
        _write()
    logger.info("New RelmonRequest inserted")


def update(request_id):
    #Updating only statuses
    logger.info("Updating RelmonRequest " + str(request_id))
    with lock:
        relmons[request_id].lastUpdate = int(time.time())
        _write()
    logger.info("RelmonRequest updated")

def updateEntireRequest(request_id, req_data):
    logger.info("Updating RelmonRequest " + str(request_id))
    with lock:
        relmons[request_id] = req_data
        relmons[request_id].lastUpdate = int(time.time())
        _write()
    logger.info("RelmonRequest updated")

def drop(request_id):
    logger.info("Dropping RelmonRequest " + str(request_id))
    with lock:
        os.chdir(os.path.join(CONFIG.SERVICE_WORKING_DIR, "logs"))
        if (os.path.isfile(str(request_id)+".log")):
            os.remove((str(request_id)+".log"))
        os.chdir(CONFIG.SERVICE_WORKING_DIR)
        relmons.pop(request_id)
        _write()
    logger.info("RelmonRequest droped")


def _write():
    logger.info("Writing to data file")
    with open(CONFIG.DATA_FILE_NAME, 'w') as json_file:
        tmp_data = []
        for el in relmons:
            tmp_data.append(relmons[el].to_dict())
        json_file.write(json.dumps(tmp_data, indent=4))
        del(tmp_data)

