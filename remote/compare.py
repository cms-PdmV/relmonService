#!/usr/bin/env python
"""
Script launches RelMon report production,
       executes report compression and
       moves report files to afs
"""
import json
import os
import argparse
import httplib
import subprocess
import shutil
from common import utils


# TODO: move hardcoded values to config file
CERTIFICATE_PATH = "/afs/cern.ch/user/j/jdaugala/.globus/usercert.pem"
KEY_PATH = "/afs/cern.ch/user/j/jdaugala/.globus/userkey.pem"
LOG_DIR_AT_SERVICE = (
    "/home/relmon/relmon_request_service/static/validation_logs/")
SERVICE_HOST = "188.184.185.27"
CREDENTIALS_PATH = "/afs/cern.ch/user/j/jdaugala/private/credentials"
USER = "jdaugala"
RELMON_PATH = (
    "/afs/cern.ch/cms/offline/dqm/ReleaseMonitoring-TEST/jdaugalaSandbox/")

# read credentials
credentials = {}
with open(CREDENTIALS_PATH) as cred_file:
    credentials = json.load(cred_file)

# parse args
parser = argparse.ArgumentParser()
parser.add_argument(dest="id", help="FIXME: id help")
# TODO: other arguments
args = parser.parse_args()

# get RelMon
status, data = utils.httpget(SERVICE_HOST,
                             "/requests/" + args.id)
if (status != httplib.OK):
    # FIXME: solve this problem
    exit()
relmon_request = json.loads(data)

# work dir and log file
local_relmon_request = os.path.abspath(
    "requests/" + str(relmon_request["id"]))
os.chdir(local_relmon_request)

local_reports = local_relmon_request + "/reports/"

# TODO: handle failures
logFile = open(str(relmon_request["id"]) + ".log", "w")
os.chmod(str(relmon_request["id"]) + ".log", 0664)

remote_reports = RELMON_PATH + relmon_request["name"] + '/'


def upload_log():
    global logFile, relmon_request
    scp_proc = subprocess.Popen(
        ["scp", "-p",
         logFile.name,
         USER + "@" + SERVICE_HOST + ":" + LOG_DIR_AT_SERVICE])
    scp_proc_return = scp_proc.wait()
    if (scp_proc_return != 0):
        # TODO: something more useful
        print("scp fail")
    status, data = utils.httpp(
        "PUT",
        SERVICE_HOST,
        "/requests/" + args.id + "/log",
        data=json.dumps({"value": True}))
    if (status != httplib.OK):
        # FIXME: solve this problem
        print("PUT about log fail")


def put_status(status):
    global logFile, relmon_request
    status, data = utils.httpp(
        "PUT",
        SERVICE_HOST,
        "/requests/" + args.id + "/status",
        data=json.dumps({"value": status}))
    if (status != httplib.OK):
        # FIXME: solve this problem
        print("konkreciai juokutis")


def finalize_report_generation(status):
    global logFile
    logFile.close()
    upload_log()
    put_status(status)


def get_local_subreport_path(category_name, HLT):
    path = local_reports + category_name
    if (HLT):
        path += "_HLT"
    return path


def validate(category_name, HLT):
    local_subreport = get_local_subreport_path(category_name, HLT)
    # TODO: handle dirs creation failures
    if (not os.path.exists(local_subreport)):
        os.makedirs(local_subreport)
        os.chmod(local_subreport, 0775)
    validation_cmd = ["ValidationMatrix.py",
                      "-a", os.path.abspath(category_name),
                      "-o", os.path.abspath(local_subreport),
                      "-N 6",
                      "--hash_name"]
    if (HLT):
        validation_cmd.append("--HLT")
    logFile.write("!       SUBPROCESS: " + " ".join(validation_cmd) + "\n")
    logFile.flush()
    # return process exit code
    return subprocess.Popen(
        validation_cmd, stdout=logFile, stderr=logFile).wait()


def compress(category_name, HLT):
    local_subreport = get_local_subreport_path(category_name, HLT)
    dir2webdir_cmd = ['dir2webdir.py', local_subreport]
    logFile.write("!       SUBPROCESS:" + " ".join(dir2webdir_cmd) + "\n")
    logFile.flush()
    return (subprocess.Popen(dir2webdir_cmd,
                             stdout=logFile,
                             stderr=logFile)
            .wait())


def move_to_afs(category_name, HLT):
    local_subreport = get_local_subreport_path(category_name, HLT)
    remote_subreport = remote_reports + category_name
    if (HLT):
        remote_subreport += "_HLT"
    # TODO: handle failures
    if (os.path.exists(remote_subreport)):
        shutil.rmtree(remote_subreport)
    shutil.copytree(local_subreport, remote_subreport)

for category in relmon_request["categories"]:
    if (not category["lists"]["target"]):
        continue
    if (category["name"] == "Generator" or category["HLT"] != "only"):
        # validate and compress; failure if either task failed
        if ((validate(category["name"], False) != 0) or
            (compress(category["name"], False) != 0)):
            # then:
            finalize_report_generation("failed")
            exit(1)
        move_to_afs(category["name"], False)
    if (category["name"] == "Generator"):
        continue
    if (category["HLT"] != "no"):
        if ((validate(category["name"], True) != 0) or
            (compress(category["name"], True) != 0)):
            finalize_report_generation("failed")
            exit(1)
        move_to_afs(category["name"], True)
finalize_report_generation("finished")
# TODO: cleanup
