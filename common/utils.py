"""
Helper functions for relmon request service.
"""

import re
import requests
import json
import paramiko

DQM_ROOT_URL = "https://cmsweb.cern.ch/dqm/relval/data/browse/ROOT/"
HYPERLINK_REGEX = re.compile(r"href=['\"]([-./\w]*)['\"]")
CERTIFICATE_PATH = "/afs/cern.ch/user/j/jdaugala/.globus/usercert.pem"
KEY_PATH = "/afs/cern.ch/user/j/jdaugala/.globus/userkey.pem"
CMSWEB_URL = "https://cmsweb.cern.ch"
DATATIER_CHECK_URL =\
    "https://cmsweb.cern.ch/reqmgr/reqMgr/outputDatasetsByRequestName/"
CREDENTIALS_PATH = "/afs/cern.ch/user/j/jdaugala/private/credentials"
REMOTE_WORK_DIR = "/build/jdaugala/relmon"

credentials = {}
with open(CREDENTIALS_PATH) as cred_file:
    credentials = json.load(cred_file)


# str:CMSSW -- version . E.g. "CMSSW_7_2_"
# bool:MC -- True -> Monte Carlo, False -> data
def get_ROOT_file_urls(CMSSW, category_name):
    url = DQM_ROOT_URL
    if (category_name == "Data"):
        url += "RelValData/"
    else:
        url += "RelVal/"
    CMSSW = re.search("CMSSW_\d_\d_", CMSSW)
    # TODO: handle this error
    if (CMSSW is None):
        return None
    url += CMSSW.group() + "x/"
    r = requests.get(url,
                     verify=False,
                     cert=(CERTIFICATE_PATH, KEY_PATH))
    # TODO: handle failure
    if (r.status_code != requests.codes.ok):
        return None
    hyperlinks = HYPERLINK_REGEX.findall(r.text)[1:]
    for u_idx, url in enumerate(hyperlinks):
        hyperlinks[u_idx] = CMSWEB_URL + url
    return hyperlinks


# FIXME: could be NONE
def get_DQMIO_datatier_name(sample_name):
    r = requests.get(DATATIER_CHECK_URL + sample_name,
                     verify=False,
                     cert=(CERTIFICATE_PATH, KEY_PATH))
    if ((r.status_code == requests.codes.ok) and ("DQMIO" in r.text)):
        rjson = json.loads(r.text.replace('\'', '\"'))
        return [i for i in rjson if "DQMIO" in i][0]
    else:
        return False


def get_ROOT_name_part(sample_name):
    DQMIO_string = get_DQMIO_datatier_name(sample_name)
    if (DQMIO_string):
        parts = DQMIO_string.split('/')[1:]
        DS = parts[0]
        CMSSW = parts[1].split('-')[0]
        PS = parts[1].split('-')[1]
        return DS + "__" + CMSSW + '-' + PS + '-'


def enough_by_status(relmon_request, status):
    with_status = 0
    for category in relmon_request["categories"]:
        for sample_list in category["lists"].itervalues():
            for sample in sample_list:
                if (sample["status"] == status):
                    with_status += 1
    return (
        (float(with_status) / float(relmon_request["sample_count"]) * 100.0) >=
        float(relmon_request["threshold"]))


# TODO: make one function instead of 2
# (launch_downloads, launch_validation_matrix)
def launch_downloads(request_id):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect("cmsdev04.cern.ch",
                username=credentials["user"],
                password=credentials["pass"])
    cmd = ("cd " +
           REMOTE_WORK_DIR +
           "; nohup ./download_DQM_ROOT.py " +
           str(request_id))
    (stdin, stdout, stderr) = ssh.exec_command(cmd)
    print (stdout.readlines())
    print (stderr.readlines())
    ssh.close()


def launch_validation_matrix(request_id):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect("cmsdev04.cern.ch",
                username=credentials["user"],
                password=credentials["pass"])
    cmd = ("cd /build/jdaugala/CMSSW_7_4_0_pre6\n eval `scramv1 runtime -sh`\n" +
           "cd " +
           REMOTE_WORK_DIR +
           "\n nohup ./compare.py " +
           str(request_id))
    (stdin, stdout, stderr) = ssh.exec_command(cmd)
    print (stdout.readlines())
    print (stderr.readlines())
    ssh.close()
