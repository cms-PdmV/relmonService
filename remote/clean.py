#!/usr/bin/env python
"""
Script for cleaning RelMon report generation products
"""

import os
import argparse
import httplib
import json
import shutil
from common import utils, relmon

# TODO: move hardcoded values to config file
RELMON_PATH = (
    "/afs/cern.ch/cms/offline/dqm/ReleaseMonitoring-TEST/jdaugalaSandbox/")
SERVICE_HOST = "188.184.185.27"

# parse arguments
parser = argparse.ArgumentParser()
parser.add_argument(dest="id_", help="FIXME: id help")
args = parser.parse_args()

# get RelMon
status, data = utils.httpget(SERVICE_HOST, "/requests/" + args.id_)
if (status != httplib.OK):
    # FIXME: solve this problem
    exit(1)
request = relmon.RelmonRequest(**json.loads(data))


def send_delete_terminator():
    status, data = utils.http(
        "DELETE",
        SERVICE_HOST,
        "/requests/" + str(request.id_) + "/terminator")
    if (status != httplib.OK):
        # FIXME: solve this problem
        print("konkreciai juokutis")

# do cleaning
if (os.path.exists("requests/" + str(request.id_))):
    shutil.rmtree("requests/" + str(request.id_))
if (not os.path.exists(RELMON_PATH + request.name + '/')):
    send_delete_terminator()
    # exit normally
    exit()
all_categories_removed = True
for category in request.categories:
    cat_report_path = (
        RELMON_PATH + request.name + '/' + category["name"])
    if (category["name"] == "Generator" or category["HLT"] != "only"):
        if (os.path.exists(cat_report_path)):
            if (len(category["lists"]["target"]) > 0):
                shutil.rmtree(cat_report_path)
            else:
                all_categories_removed = False
    if (category["HLT"] != "no" and category["name"] != "Generator"):
        cat_report_path += "_HLT"
        if (os.path.exists(cat_report_path)):
            if (len(category["lists"]["target"]) > 0):
                shutil.rmtree(cat_report_path)
            else:
                all_categories_removed = False
if (all_categories_removed):
    shutil.rmtree(RELMON_PATH + request.name + '/')
send_delete_terminator()
