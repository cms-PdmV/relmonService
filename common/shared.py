"""
Shared variables and data file api.
"""
from common import relmon
import os
import threading
import json
import config as CONFIG

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
    global relmons, controllers
    with lock:
        if (request.id_ in relmons.keys()):
            raise KeyError("RelmonRequest with given id_ already exists")
        relmons[request.id_] = request
        relmon_data.insert(0, request.to_dict())
        _write()


def update(request_id):
    with lock:
        for req_idx, request in enumerate(relmon_data):
            if (request["id_"] == request_id):
                relmon_data[req_idx] = relmons[request_id].to_dict()
                break
        _write()


def drop(request_id):
    with lock:
        relmons.pop(request_id)
        for req_idx, request in enumerate(relmon_data):
            if (request["id_"] == request_id):
                relmon_data.pop(req_idx)
                break
        _write()


def _write():
    with open(CONFIG.DATA_FILE_NAME, 'w') as json_file:
        json_file.write(json.dumps(relmon_data, indent=4))
