"""
Global relmon request service data variable
"""
import os
from threading import RLock
import json

RR_data = []
RR_data_lock = RLock()

with RR_data_lock:
    if not os.path.isfile("data"):
        with open("data", 'w') as new_file:
            new_file.write("[]")
    else:
        with open("data") as json_file:
            RR_data = json.load(json_file)


def write_RR_data():
    with RR_data_lock:
        with open("data", 'w') as json_file:
            json_file.write(json.dumps(RR_data, indent=4))
