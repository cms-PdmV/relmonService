#!/usr/bin/env python
"""

"""
import json
import os
import argparse
import httplib
import subprocess

CERTIFICATE_PATH = "/afs/cern.ch/user/j/jdaugala/.globus/usercert.pem"
KEY_PATH = "/afs/cern.ch/user/j/jdaugala/.globus/userkey.pem"

parser = argparse.ArgumentParser()
parser.add_argument(dest="id", help="FIXME: id help")
# TODO: other arguments
args = parser.parse_args()

conn = httplib.HTTPConnection("188.184.185.27", 80)
conn.connect()
conn.request('GET', "/requests/" + args.id)
response = conn.getresponse()
if (response.status != httplib.OK):
    # FIXME: solve this problem
    exit()
relmon_request = json.loads(response.read())
conn.close()

rr_path = "requests/" + str(relmon_request["id"])
os.chdir(rr_path)
for category in relmon_request["categories"]:
    print("iteration")
    command = ["ValidationMatrix.py",
               "-a",
               category["name"],
               "-o",
               category["name"] + "Report",
               "--hash_name"]
    if (category["HLT"]):
        command.append("--HLT")
    print(" ".join(command))
    proc = subprocess.Popen(command)
    proc.wait()
    print("something schould have happened")
    print(category["name"])
