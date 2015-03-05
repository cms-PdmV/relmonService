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


# Given sample name returns DQMIO dataset status. If given sample
# produces DQMIO dataset then the second object of the returned
# tuple is the name of that DQMIO dataset
def get_DQMIO_status(sample_name):
    r = requests.get(DATATIER_CHECK_URL + sample_name,
                     verify=False,
                     cert=(CERTIFICATE_PATH, KEY_PATH))
    if (r.status_code == requests.codes.ok):
        if ("DQMIO" in r.text):
            rjson = json.loads(r.text.replace('\'', '\"'))
            return ("DQMIO", [i for i in rjson if "DQMIO" in i][0])
        # TODO: make "waiting" check real (this is a guess)
        if ("None" in r.text):
            return ("waiting", None)
    return ("NoDQMIO", None)


def get_ROOT_name_part(DQMIO_string):
    if (DQMIO_string):
        parts = DQMIO_string.split('/')[1:]
        DS = parts[0]
        CMSSW = parts[1].split('-')[0]
        PS = parts[1].split('-')[1]
        return DS + "__" + CMSSW + '-' + PS + '-'


def sample_percent_by_status(relmon_request, status, ignore):
    not_ignored = 0
    with_status = 0
    for category in relmon_request["categories"]:
        for sample_list in category["lists"].itervalues():
            for sample in sample_list:
                if (sample["status"] in ignore):
                    continue
                if (sample["status"] in status):
                    with_status += 1
                not_ignored += 1
    if (not_ignored == 0):
        return 0.0
    return (float(with_status) / float(not_ignored) * 100.0)


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
