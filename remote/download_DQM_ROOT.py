#!/usr/bin/env python
"""
ROOT files downloader (and reporter) for given relmon
request id (from relmon request service).
"""

import os
import argparse
import httplib
import json
from common import utils

# TODO: move hardcoded values to config file
CERTIFICATE_PATH = "/afs/cern.ch/user/j/jdaugala/.globus/usercert.pem"
KEY_PATH = "/afs/cern.ch/user/j/jdaugala/.globus/userkey.pem"
SERVICE_HOST = "188.184.185.27"
CMSWEB_HOST = "cmsweb.cern.ch"

parser = argparse.ArgumentParser()
parser.add_argument(dest="id", help="FIXME: id help")
# parser.add_argument("--")
# parser.add_argument("--dry", dest="dry", action="store_true", default=False)
args = parser.parse_args()

status, data = utils.httpget(SERVICE_HOST, "/requests/" + str(args.id))
if (status != httplib.OK):
    # FIXME: solve this problem
    exit()
relmon_request = json.loads(data)

rr_path = "requests/" + str(relmon_request["id"])
original_umask = os.umask(0)
if (not os.path.exists(rr_path)):
    os.makedirs(rr_path, 0770)
os.chdir(rr_path)
for category in relmon_request["categories"]:
    if (not os.path.exists(category["name"])):
        os.makedirs(category["name"], 0770)
    os.chdir(category["name"])
    for lname, sample_list in category["lists"].iteritems():
        if (not sample_list):
            continue
        # NOTE: ref and target samples in the same
        # directory for automatic pairing
        #
        # if (not os.path.exists(lname)):
        #     os.makedirs(lname, 0770)
        # os.chdir(lname)
        # TODO: handle failures
        file_urls = utils.get_ROOT_file_urls(
            sample_list[0]["name"],
            category["name"])
        for sample in sample_list:
            print(sample["name"])
            if (sample["status"] != "ROOT"):
                print("Not ROOT. Skipping")
                continue
            assert "jdaugala" in os.getcwd()
            downloaded_count = 0
            for file_url in file_urls:
                if (sample["ROOT_file_name_part"] not in file_url):
                    continue
                # TODO: handle failures (httpsget_large_file)
                utils.httpsget_large_file(file_url.split("/")[-1],
                                          CMSWEB_HOST,
                                          file_url)
                downloaded_count += 1
            # <- end of file_urls
            print(downloaded_count)
            # Maybe do something else with the downloaded_count
            if (downloaded_count > 0):
                sample["status"] = "downloaded"
                headers = {
                    "Content-type": "application/json",
                    "Accept": "text/plain"}
                # TODO: handle failures (request)
                status, data = utils.httpp(
                    "PUT",
                    SERVICE_HOST,
                    "/requests/" + args.id +
                    "/categories/" + category["name"] +
                    "/lists/" + lname +
                    "/samples/" + sample["name"],
                    data=json.dumps(sample))
        # <- end of samples
        # NOTE: same dir for ref and target
        # os.chdir("..")
    # <- end of lists
    os.chdir("..")
# <- end of categories
os.umask(original_umask)
