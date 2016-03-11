#!/usr/bin/env python
"""ROOT files downloader (and reporter) for given relmon request id
(from relmon request service)
"""

import logging
import logging.handlers
import os
import argparse
import httplib
import json

from config import CONFIG
from common import utils, relmon


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.handlers.RotatingFileHandler(
    "download_ROOT.log", mode='a', maxBytes=10485760, backupCount=4)
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)

def get_first_existing_sample(sample_list):
    for sample in sample_list:
        if (sample["DQMIO_string"] != None):
            return sample

parser = argparse.ArgumentParser()
parser.add_argument(dest="id_", help="FIXME: id help")
# parser.add_argument("--")
# parser.add_argument("--dry", dest="dry", action="store_true", default=False)
args = parser.parse_args()

# get relmon
cookie = utils.get_sso_cookie(CONFIG.SERVICE_HOST)
if (cookie is None):
    logger.error("Failed getting sso cookies for " + CONFIG.SERVICE_HOST)
    exit(1)
status, data = utils.httpsget(
    CONFIG.SERVICE_HOST,
    CONFIG.SERVICE_BASE + "/requests/" + str(args.id_),
    headers={"Cookie": cookie})

if (status != httplib.OK):
    # FIXME: solve this problem
    logger.error("Failed getting RelMon request. Status: " + str(status))
    exit(1)
request = relmon.RelmonRequest(**json.loads(data))

rr_path = os.path.join("requests", str(request.id_))
original_umask = os.umask(0)
if (not os.path.exists(rr_path)):
    os.makedirs(rr_path, 0770)
os.chdir(rr_path)
for category in request.categories:

    if (not category["lists"]["target"]):
        continue
    if (not os.path.exists(category["name"])):
        os.makedirs(category["name"], 0770)
    os.chdir(category["name"])
    for lname, sample_list in category["lists"].iteritems():
        # NOTE: ref and target samples in the same
        # directory for automatic pairing
        #
        # if (not os.path.exists(lname)):
        #     os.makedirs(lname, 0770)
        # os.chdir(lname)
        # TODO: handle failures

        existing_sample = get_first_existing_sample(sample_list)
        if (not existing_sample):
            continue
        file_urls = utils.get_ROOT_file_urls(
            existing_sample["name"],
            category["name"])
        for sample in sample_list:
            if (sample["status"] != "ROOT"):
                continue
            file_count = 0

            for file_url in file_urls:
                if (sample["ROOT_file_name_part"] not in file_url):
                    continue
                # TODO: handle failures (httpsget_large_file)
                if (os.path.isfile(file_url.split("/")[-1])):
                    file_count += 1
                    
                # TODO: handle failures (httpsget_large_file)
                else:
                    utils.httpsget_large_file(file_url.split("/")[-1],
                                          CONFIG.CMSWEB_HOST,
                                          file_url)
                    file_count += 1

            # <- end of file_urls
            # Maybe do something else with the downloaded_count
                if (file_count == sample["run_count"]):
                    sample["status"] = "downloaded"
                    # TODO: handle failures (request)
                    cookie = utils.get_sso_cookie(CONFIG.SERVICE_HOST)
                    if (cookie is None):
                        logger.error("Failed getting sso cookies for " +
                                     CONFIG.SERVICE_HOST)
                        # exit(1)
                    status, data = utils.https(
                        "PUT",
                        CONFIG.SERVICE_HOST,
                        CONFIG.SERVICE_BASE + "/requests/" +
                        str(request.id_) + "/categories/" + category["name"] +
                        "/lists/" + lname + "/samples/" + sample["name"],
                        data=json.dumps(sample),
                        headers={"Cookie": cookie})
        # <- end of samples
        # NOTE: same dir for ref and target
        # os.chdir("..")
    # <- end of lists
    os.chdir("..")
# <- end of categories
os.umask(original_umask)