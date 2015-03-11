#!/usr/bin/env python
"""
ROOT files downloader (and reporter) for given relmon
request id (from relmon request service).
"""

import os
import argparse
import requests
from common import utils

CERTIFICATE_PATH = "/afs/cern.ch/user/j/jdaugala/.globus/usercert.pem"
KEY_PATH = "/afs/cern.ch/user/j/jdaugala/.globus/userkey.pem"

parser = argparse.ArgumentParser()
parser.add_argument(dest="id", help="FIXME: id help")
# parser.add_argument("--")
# parser.add_argument("--dry", dest="dry", action="store_true", default=False)
args = parser.parse_args()

req = requests.get("http://188.184.185.27/requests/" + args.id)
if (req.status_code != requests.codes.ok):
    # FIXME: solve this problem
    exit()
relmon_request = req.json()
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
        print(sample_list)
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
        # ^
        for sample in sample_list:
            print(sample["name"])
            if (sample["status"] != "ROOT"):
                print("not root")
                continue
            if ("jdaugala" not in os.getcwd()):
                print("pz... Jono scriptas vel mala suda...")
                exit()
            downloaded_count = 0
            for file_url in file_urls:
                if (sample["ROOT_file_name_part"] not in file_url):
                    continue
                print("trying to download")
                # stream=True -- for large files
                r = requests.get(file_url,
                                 stream=True,
                                 verify=False,
                                 cert=(CERTIFICATE_PATH, KEY_PATH))
                with open(file_url.split("/")[-1], 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024):
                        if (chunk):  # filter out keep-alive new chunks
                            f.write(chunk)
                            f.flush()
                    if (f):
                        print("succesful downloading")
                        downloaded_count += 1
            # Maybe do something else with the downloaded_count
            if (downloaded_count > 0):
                sample["status"] = "downloaded"
                headers = {
                    "Content-type": "application/json",
                    "Accept": "text/plain"
                }
                # TODO: handle failures
                print("vizkas zjbys")
                r = requests.put(
                    "http://188.184.185.27/requests/" + args.id +
                    "/categories/" + category["name"] +
                    "/lists/" + lname +
                    "/samples/" + sample["name"],
                    json=sample,
                    headers=headers
                )
                print(r.status_code)
                # ^
        # NOTE: same dir for ref and target
        # os.chdir("..")
    os.chdir("..")
os.umask(original_umask)
