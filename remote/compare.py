#!/usr/bin/env python
"""

"""
import json
import os
import argparse
import httplib
import subprocess
import shutil

# TODO: move hardcoded values to config file
CERTIFICATE_PATH = "/afs/cern.ch/user/j/jdaugala/.globus/usercert.pem"
KEY_PATH = "/afs/cern.ch/user/j/jdaugala/.globus/userkey.pem"
LOG_DIR_AT_SERVICE = (
    "/home/relmon/relmon_request_service/static/validation_logs/")
SERVICE_IP = "188.184.185.27"
CREDENTIALS_PATH = "/afs/cern.ch/user/j/jdaugala/private/credentials"
USER = "jdaugala"
RELMON_PATH = (
    "/afs/cern.ch/cms/offline/dqm/ReleaseMonitoring-TEST/jdaugalaSandbox/")

credentials = {}
with open(CREDENTIALS_PATH) as cred_file:
    credentials = json.load(cred_file)

parser = argparse.ArgumentParser()
parser.add_argument(dest="id", help="FIXME: id help")
# TODO: other arguments
args = parser.parse_args()
# TODO: function for GET
conn = httplib.HTTPConnection(SERVICE_IP, 80)
conn.connect()
conn.request('GET', "/requests/" + args.id)
response = conn.getresponse()
if (response.status != httplib.OK):
    # FIXME: solve this problem
    exit()
relmon_request = json.loads(response.read())
conn.close()
rr_path = os.path.abspath("requests/" + str(relmon_request["id"]))
os.chdir(rr_path)
# TODO: handle failures
logFile = open(str(relmon_request["id"]) + ".log", "w")
os.chmod(str(relmon_request["id"]) + ".log", 0664)


# TODO: function for PUT
def upload_log():
    global logFile, relmon_request
    scp_proc = subprocess.Popen(
        ["scp", "-p",
         logFile.name,
         USER + "@" + SERVICE_IP + ":" + LOG_DIR_AT_SERVICE])
    scp_proc_return = scp_proc.wait()
    if (scp_proc_return != 0):
        # TODO: something more useful
        print("scp fail")
    conn = httplib.HTTPConnection(SERVICE_IP, 80)
    conn.connect()
    headers = {"Content-type": "application/json"}
    conn.request("PUT",
                 "/requests/" + str(relmon_request["id"]) + "/log",
                 json.dumps({"value": True}),
                 headers)
    response = conn.getresponse()
    conn.close()
    if (response.status != httplib.OK):
        # FIXME: solve this problem
        print("PUT about log fail")


def put_status(status):
    global logFile, relmon_request
    conn = httplib.HTTPConnection(SERVICE_IP, 80)
    conn.connect()
    headers = {"Content-type": "application/json"}
    conn.request("PUT",
                 "/requests/" + str(relmon_request["id"]) + "/status",
                 json.dumps({"value": status}),
                 headers)
    response = conn.getresponse()
    conn.close()
    if (response.status != httplib.OK):
        # FIXME: solve this problem
        print("konkreciai juokutis")


def finalize_report_generation(status):
    global logFile
    logFile.close()
    upload_log()
    put_status(status)


report_path = RELMON_PATH + relmon_request["name"] + '/'


for category in relmon_request["categories"]:
    if (not category["lists"]["target"]):
        continue
    # TODO: handle dirs creation failure
    if (not os.path.exists("reports/" + category["name"])):
        os.makedirs("reports/" + category["name"])
        os.chmod("reports/" + category["name"], 0775)
    validation_cmd = ["ValidationMatrix.py",
                      "-a",
                      os.path.abspath(category["name"]),
                      "-o",
                      os.path.abspath("reports/" + category["name"]),
                      "-N 6",
                      "--hash_name"]
    if (category["HLT"]):
        validation_cmd.append("--HLT")
    logFile.write("!       SUBPROCESS: " + " ".join(validation_cmd) + "\n")
    logFile.flush()
    v_proc = subprocess.Popen(validation_cmd, stdout=logFile, stderr=logFile)
    v_proc_return = v_proc.wait()
    if (v_proc_return != 0):
        finalize_report_generation("failed")
        exit()
    dir2webdir_cmd = ['dir2webdir.py', "reports/" + category["name"]]
    logFile.write("!       SUBPROCESS:" + " ".join(dir2webdir_cmd) + "\n")
    logFile.flush()
    d2w_proc = subprocess.Popen(dir2webdir_cmd, stdout=logFile, stderr=logFile)
    d2w_proc_return = d2w_proc.wait()
    if (d2w_proc_return != 0):
        finalize_report_generation("failed")
        exit()
    # TODO: handle failures
    shutil.copytree("reports/" + category["name"],
                    report_path + category["name"])
finalize_report_generation("finished")
# TODO: cleanup
