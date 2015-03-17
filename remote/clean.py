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

parser = argparse.ArgumentParser()
parser.add_argument(dest="id", help="FIXME: id help")
args = parser.parse_args()

# get RelMon
status, data = utils.httpget(SERVICE_HOST,
                             "/requests/" + args.id)
if (status != httplib.OK):
    # FIXME: solve this problem
    exit()
relmon_request = json.loads(data)

if (os.path.exists("requests/" + args.id)):
    shutil.rmtree("requests/" + args.id)
report_path = RELMON_PATH + relmon_request["name"] + '/'
if (os.path.exists(report_path)):
    shutil.rmtree(report_path)
