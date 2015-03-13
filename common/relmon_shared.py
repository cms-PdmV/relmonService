"""
Global relmon request service data variable
"""
import os
from threading import RLock
import json

DATA_FILE_NAME = "data"
data = []
data_lock = RLock()
reporters = {}
downloaders = {}
terminators = {}

with data_lock:
    if not os.path.isfile(DATA_FILE_NAME):
        with open(DATA_FILE_NAME, 'w') as new_file:
            new_file.write("[]")
    else:
        with open(DATA_FILE_NAME) as json_file:
            data = json.load(json_file)


def write_data():
    with data_lock:
        with open(DATA_FILE_NAME, 'w') as json_file:
            json_file.write(json.dumps(data, indent=4))
