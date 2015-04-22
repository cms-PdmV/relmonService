#!/usr/bin/env python
"""Script launches RelMon report production,
        executes report compression and
        moves report files to afs
"""

import logging
import logging.handlers
import json
import os
import argparse
import httplib
import subprocess
import shutil

from config import CONFIG
from common import utils, relmon


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.handlers.RotatingFileHandler(
    "compare.log", mode='a', maxBytes=10485760, backupCount=4)
handler.setLevel(logging.INFO)
logger.addHandler(handler)

# read credentials
credentials = {}
with open(CONFIG.CREDENTIALS_PATH) as cred_file:
    credentials = json.load(cred_file)

# parse args
parser = argparse.ArgumentParser()
parser.add_argument(dest="id_", help="FIXME: id help")
# TODO: other arguments
args = parser.parse_args()

# get RelMon
cookie = utils.get_sso_cookie(CONFIG.SERVICE_HOST)
if (cookie is None):
    logger.error("Failed getting sso cookies for " + CONFIG.SERVICE_HOST)
    exit(1)
status, data = utils.httpsget(
    CONFIG.SERVICE_HOST,
    CONFIG.SERVICE_BASE + "/requests/" + args.id_,
    headers={"Cookie": cookie})
if (status != httplib.OK):
    # FIXME: solve this problem
    exit(1)
request = relmon.RelmonRequest(**json.loads(data))

# work dir and log file
local_relmon_request = os.path.abspath(
    os.path.join("requests", str(request.id_)))
os.chdir(local_relmon_request)

local_reports = os.path.join(local_relmon_request, "reports")

# TODO: handle failures
logFile = open(str(request.id_) + ".log", "w")
os.chmod(str(request.id_) + ".log", 0664)

remote_reports = os.path.join(CONFIG.RELMON_PATH, request.name)


def upload_log():
    global request
    cookie = utils.get_sso_cookie(CONFIG.SERVICE_HOST)
    if (cookie is None):
        logger.error("Failed getting sso cookies for " + CONFIG.SERVICE_HOST)
        exit(1)
    status, data = utils.https(
        "POST",
        CONFIG.SERVICE_HOST,
        CONFIG.SERVICE_BASE + "/requests/" + str(request.id_) + "/log",
        data=open(str(request.id_) + ".log", "rb"),
        headers={"Cookie": cookie})
    if (status != httplib.OK):
        # FIXME: solve this problem
        print("PUT log fail")


# TODO: think of other ways for controllers to know about failures/success
def put_status(status):
    global logFile, request
    cookie = utils.get_sso_cookie(CONFIG.SERVICE_HOST)
    if (cookie is None):
        logger.error("Failed getting sso cookies for " + CONFIG.SERVICE_HOST)
        exit(1)
    status, data = utils.https(
        "PUT",
        CONFIG.SERVICE_HOST,
        CONFIG.SERVICE_BASE + "/requests/" + str(request.id_) + "/status",
        data=json.dumps({"value": status}),
        headers={"Cookie": cookie})
    if (status != httplib.OK):
        # FIXME: solve this problem
        logger.error("Failed updating status")


def finalize_report_generation(status):
    logger.info("Finalizing RelMon report production with satus: " + status)
    global logFile
    logFile.close()
    upload_log()
    put_status(status)
    os.chdir(os.path.dirname(local_relmon_request))
    shutil.rmtree(local_relmon_request)


def get_local_subreport_path(category_name, HLT):
    name = category_name
    if ("PU" in category_name):
        name = name.split('_')[0] + "Report_PU"
    else:
        name += "Report"
    path = os.path.join(local_reports, name)
    if (HLT):
        path += "_HLT"
    return path


def validate(category_name, HLT):
    global logFile
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
    remote_subreport = os.path.join(remote_reports,
                                    os.path.basename(local_subreport))
    # TODO: handle failures
    if (os.path.exists(remote_subreport)):
        shutil.rmtree(remote_subreport)
    shutil.copytree(local_subreport, remote_subreport)

for category in request.categories:
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
