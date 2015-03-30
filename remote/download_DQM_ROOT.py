#!/usr/bin/env python
"""
ROOT files downloader (and reporter) for given relmon
request id (from relmon request service).
"""
import os
import argparse
import httplib
import json
from common import utils, relmon
import config as CONFIG

parser = argparse.ArgumentParser()
parser.add_argument(dest="id_", help="FIXME: id help")
# parser.add_argument("--")
# parser.add_argument("--dry", dest="dry", action="store_true", default=False)
args = parser.parse_args()

status, data = utils.httpget(
    CONFIG.SERVICE_HOST, "/requests/" + str(args.id_))
print(status)
print(data)
if (status != httplib.OK):
    # FIXME: solve this problem
    exit(1)
request = relmon.RelmonRequest(**json.loads(data))

rr_path = "requests/" + str(request.id_)
original_umask = os.umask(0)
if (not os.path.exists(rr_path)):
    os.makedirs(rr_path, 0770)
os.chdir(rr_path)
for category in request.categories:
    print(category["name"])
    print(category["lists"]["target"])
    if (not category["lists"]["target"]):
        continue
    if (not os.path.exists(category["name"])):
        os.makedirs(category["name"], 0770)
    os.chdir(category["name"])
    for lname, sample_list in category["lists"].iteritems():
        print(lname)
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
            if (sample["status"] != "ROOT"):
                continue
            file_count = 0
            for file_url in file_urls:
                if (sample["ROOT_file_name_part"] not in file_url):
                    continue
                # TODO: handle failures (httpsget_large_file)
                if (os.path.isfile(file_url.split("/")[-1])):
                    file_count += 1
                    continue
                # TODO: handle failures (httpsget_large_file)
                utils.httpsget_large_file(file_url.split("/")[-1],
                                          CONFIG.CMSWEB_HOST,
                                          file_url)
                file_count += 1

            # <- end of file_urls
            # Maybe do something else with the downloaded_count
            print(file_count)
            print(sample["run_count"])
            if (file_count == sample["run_count"]):
                sample["status"] = "downloaded"
                headers = {
                    "Content-type": "application/json",
                    "Accept": "text/plain"}
                # TODO: handle failures (request)
                status, data = utils.http(
                    "PUT",
                    CONFIG.SERVICE_HOST,
                    ("/requests/" + str(request.id_) + "/categories/" +
                     category["name"] + "/lists/" + lname + "/samples/" +
                     sample["name"]),
                    data=json.dumps(sample))
        # <- end of samples
        # NOTE: same dir for ref and target
        # os.chdir("..")
    # <- end of lists
    os.chdir("..")
# <- end of categories
os.umask(original_umask)
