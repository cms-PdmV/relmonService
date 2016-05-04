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

def get_first_existing_sample(sample_list):
    for sample in sample_list:
        if (sample["DQMIO_string"] != None):
            return sample

class IdFilter(logging.Filter):
    """
    This is a filter which injects contextual information into the log.
    """

    def filter(self, record):

        if args.id_:
            record.id_ = args.id_
        else:
            record.id_ = "main_thread"
        return True


parser = argparse.ArgumentParser()
parser.add_argument(dest="id_", help="FIXME: id help")
args = parser.parse_args()

##get logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

#define logger handler and formatter
handler = logging.handlers.RotatingFileHandler(
        "download_ROOT.log", maxBytes=10485760, backupCount=4)

formatter = logging.Formatter(fmt='[%(asctime)s][%(id_)s][%(levelname)s] %(message)s',
        datefmt='%d/%b/%Y:%H:%M:%S')

id_filt = IdFilter()

##set all logger module
handler.setFormatter(formatter)
handler.addFilter(id_filt)
logger.addHandler(handler)

# get relmon
cookie = None
cookie = utils.get_sso_cookie(CONFIG.SERVICE_HOST)
if (cookie is None):
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
            existing_sample["DQMIO_string"],
            category["name"])
        for sample in sample_list:
            logger.debug("sample::: %s " %sample["name"])
            if (sample["status"] != "ROOT"):
                continue
            file_count = 0

            for file_url in file_urls:
                if (sample["ROOT_file_name_part"] not in file_url):
                    logger.debug("file_name_part not in URL")
                    continue
                # TODO: handle failures (httpsget_large_file)
                if (os.path.isfile(file_url.split("/")[-1])):
                    logger.debug("file already exist: %s" %sample["name"])
                    file_count += 1

                # TODO: handle failures (httpsget_large_file)
                else:
                    logger.debug("downloading file: %s" %sample["name"])
                    utils.httpsget_large_file(file_url.split("/")[-1],
                                          CONFIG.CMSWEB_HOST,
                                          file_url)
                    file_count += 1


            # <- end of file_urls
            # Maybe do something else with the downloaded_count

                if ((file_count >= sample["run_count"] and sample["run_count"] != 0)):
                    sample["status"] = "downloaded"
                    status, data = utils.https(
                        "PUT",
                        CONFIG.SERVICE_HOST,
                        CONFIG.SERVICE_BASE + "/requests/" +
                        str(request.id_) + "/categories/" + category["name"] +
                        "/lists/" + lname + "/samples/" + sample["name"],
                        data=json.dumps(sample),
                        headers={"Cookie": cookie})

                elif ((file_count < sample["run_count"]) or (sample["run_count"] == 0) or (file_count < sample["root_count"])):
                    logger.debug("marking as failed download. file_cout:%s" % (file_count))
                    sample["status"] = "failed download"
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
