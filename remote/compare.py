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

# TODO: handle failures
logFile = open("validation.log", "w")

for category in relmon_request["categories"]:
    print("Started " + category["name"] + " reports.")
    # TODO: handle dirs creation failure
    os.makedirs("reports/" + category["name"])
    validation_cmd = ["ValidationMatrix.py",
                      "-a",
                      category["name"],
                      "-o",
                      "reports/" + category["name"],
                      "--hash_name"]
    if (category["HLT"]):
        validation_cmd.append("--HLT")
    print(" ".join(validation_cmd))
    # TODO: handle failures
    proc = subprocess.Popen(validation_cmd, stdout=logFile)
    proc.wait()
    dir2webdir_cmd = ['dir2webdir.py', "reports/" + category["name"]]
    logFile.write("    SUBPROCESS: " + " ".join(dir2webdir_cmd))
    print(" ".join(dir2webdir_cmd))
    proc = subprocess.Popen(dir2webdir_cmd)
    proc.wait()
    print("Finished " + category["name"] + " reports.")
