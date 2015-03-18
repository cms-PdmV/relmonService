#!/usr/bin/env python
"""
Script for cleaning RelMon report generation products
"""

import os
import argparse
import httplib
import json
import shutil
from common import utils

# TODO: move hardcoded values to config file
RELMON_PATH = (
    "/afs/cern.ch/cms/offline/dqm/ReleaseMonitoring-TEST/jdaugalaSandbox/")
SERVICE_HOST = "188.184.185.27"

# parse arguments
parser = argparse.ArgumentParser()
parser.add_argument(dest="id", help="FIXME: id help")
args = parser.parse_args()

# get RelMon
status, data = utils.httpget(SERVICE_HOST, "/requests/" + args.id)
if (status != httplib.OK):
    # FIXME: solve this problem
    exit()
relmon_request = json.loads(data)

# do cleaning
if (os.path.exists("requests/" + args.id)):
    shutil.rmtree("requests/" + args.id)
if (not os.path.exists(RELMON_PATH + relmon_request["name"] + '/')):
    exit()
all_categories_removed = True
for category in relmon_request["categories"]:
    cat_report_path = (
        RELMON_PATH + relmon_request["name"] + '/' + category["name"])
    if (not os.path.exists(cat_report_path)):
        break
    if (len(category["lists"]["target"]) < 1):
        all_categories_removed = False
        break
    shutil.rmtree(cat_report_path)
if (all_categories_removed):
    shutil.rmtree(RELMON_PATH + relmon_request["name"] + '/')
