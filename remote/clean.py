#!/usr/bin/env python
"""Script for cleaning RelMon report generation products"""

import logging
import logging.handlers
import os
import argparse
import httplib
import json
import shutil

from config import CONFIG
from common import utils, relmon

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.handlers.RotatingFileHandler(
    "clean.log", mode='a', maxBytes=10485760, backupCount=4)
handler.setLevel(logging.INFO)
logger.addHandler(handler)

# parse arguments
parser = argparse.ArgumentParser()
parser.add_argument(dest="id_", help="FIXME: id help")
args = parser.parse_args()

# get RelMon
cookie = utils.get_sso_cookie(CONFIG.SERVICE_HOST)
if (cookie is None):
    logger.error("Failed getting sso cookies for " + CONFIG.SERVICE_HOST)
    exit(1)
status, data = utils.httpsget(CONFIG.SERVICE_HOST,
                              "/requests/" + args.id_,
                              headers={"Cookie": cookie})
if (status != httplib.OK):
    # FIXME: solve this problem
    exit(1)
request = relmon.RelmonRequest(**json.loads(data))


def send_delete_terminator():
    cookie = utils.get_sso_cookie(CONFIG.SERVICE_HOST)
    if (cookie is None):
        logger.error("Failed getting sso cookies for " + CONFIG.SERVICE_HOST)
        exit(1)
    status, data = utils.https(
        "DELETE",
        CONFIG.SERVICE_HOST,
        "/requests/" + str(request.id_) + "/terminator",
        headers={"Cookie": cookie})
    if (status != httplib.OK):
        # FIXME: solve this problem
        print("konkreciai juokutis")

# do cleaning
if (os.path.exists("requests/" + str(request.id_))):
    shutil.rmtree("requests/" + str(request.id_))
if (not os.path.exists(CONFIG.RELMON_PATH + '/' + request.name + '/')):
    send_delete_terminator()
    # exit normally
    exit()
all_categories_removed = True
for category in request.categories:
    cat_report_path = (
        CONFIG.RELMON_PATH + '/' + request.name + '/' + category["name"])
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
    shutil.rmtree(CONFIG.RELMON_PATH + '/' + request.name + '/')
send_delete_terminator()
