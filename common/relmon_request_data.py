"""
Global relmon request service data variable
"""

from threading import RLock
import json

RR_data = []
RR_data_lock = RLock()

# TODO: create file if not exists
with RR_data_lock:
    with open("data") as json_file:
        RR_data = json.load(json_file)


def write_RR_data():
    with RR_data_lock:
        with open("data", 'w') as json_file:
            json_file.write(json.dumps(RR_data, indent=4))
