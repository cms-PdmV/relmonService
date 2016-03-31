#!/usr/bin/env python
"""Script launches RelMon report production,
        executes report compression and
        moves report files to afs
"""

import logging
import logging.handlers
import json
import os
import argparse
import httplib
import subprocess
import shutil
import numpy as np
from config import CONFIG
from common import utils, relmon


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.handlers.RotatingFileHandler(
    "compare.log", mode='a', maxBytes=10485760, backupCount=4)
handler.setLevel(logging.INFO)
logger.addHandler(handler)

# read credentials
credentials = {}
with open(CONFIG.CREDENTIALS_PATH) as cred_file:
    credentials = json.load(cred_file)

# parse args
parser = argparse.ArgumentParser()
parser.add_argument(dest="id_", help="FIXME: id help")
# TODO: other arguments
args = parser.parse_args()

# get RelMon
cookie = utils.get_sso_cookie(CONFIG.SERVICE_HOST)
if (cookie is None):
    logger.error("Failed getting sso cookies for " + CONFIG.SERVICE_HOST)
    exit(1)
status, data = utils.httpsget(
    CONFIG.SERVICE_HOST,
    CONFIG.SERVICE_BASE + "/requests/" + args.id_,
    headers={"Cookie": cookie})
if (status != httplib.OK):
    # FIXME: solve this problem
    exit(1)
request = relmon.RelmonRequest(**json.loads(data))

logger.info("request: %s" %request)
# work dir and log file
local_relmon_request = os.path.abspath(
    os.path.join("requests", str(request.id_)))
os.chdir(local_relmon_request)

local_reports = os.path.join(local_relmon_request, "reports")

logger.info("local_reports::: %s " %local_reports)
# TODO: handle failures
logFile = open(str(request.id_) + ".log", "w")
os.chmod(str(request.id_) + ".log", 0664)

remote_reports = os.path.join(CONFIG.RELMON_PATH, request.name)

logger.info("remote_reports:: %s" %remote_reports)
def upload_log():
    global request
    cookie = utils.get_sso_cookie(CONFIG.SERVICE_HOST)
    if (cookie is None):
        logger.error("Failed getting sso cookies for " + CONFIG.SERVICE_HOST)
        exit(1)
    status, data = utils.https(
        "POST",
        CONFIG.SERVICE_HOST,
        CONFIG.SERVICE_BASE + "/requests/" + str(request.id_) + "/log",
        data=open(str(request.id_) + ".log", "rb"),
        headers={"Cookie": cookie})
    if (status != httplib.OK):
        # FIXME: solve this problem
        print("PUT log fail")


# TODO: think of other ways for controllers to know about failures/success
def put_status(status):
    global logFile, request
    cookie = utils.get_sso_cookie(CONFIG.SERVICE_HOST)
    if (cookie is None):
        logger.error("Failed getting sso cookies for " + CONFIG.SERVICE_HOST)
        exit(1)
    status, data = utils.https(
        "PUT",
        CONFIG.SERVICE_HOST,
        CONFIG.SERVICE_BASE + "/requests/" + str(request.id_) + "/status",
        data=json.dumps({"value": status}),
        headers={"Cookie": cookie})
    if (status != httplib.OK):
        # FIXME: solve this problem
        logger.error("Failed updating status")


def finalize_report_generation(status):
    logger.info("Finalizing RelMon report production with satus: " + status)
    global logFile
    logFile.close()
    upload_log()
    put_status(status)
    os.chdir(os.path.dirname(local_relmon_request))
    shutil.rmtree(local_relmon_request)


def get_local_subreport_path(category_name, HLT):
    name = category_name
    if ("PU" in category_name):
        name = name.split('_')[0] + "Report_PU"
    else:
        name += "Report"
    path = os.path.join(local_reports, name)
    if (HLT):
        path += "_HLT"
    return path

def levenshtein(source, target):
    if len(source) < len(target):
        return levenshtein(target, source)

    if len(target) == 0:
        return len(source)

    source = np.array(tuple(source))
    target = np.array(tuple(target))

    previous_row = np.arange(target.size + 1)
    for s in source:
        current_row = previous_row + 1

        current_row[1:] = np.minimum(
                current_row[1:],
                np.add(previous_row[:-1], target != s))

        current_row[1:] = np.minimum(
                current_row[1:],
                current_row[0:-1] + 1)

        previous_row = current_row

    return previous_row[-1]

def construct_final_lists(refs, tars):
    fnl_refs = []
    fnl_tars = []
    temp = []
    tmp2 = []
    max_ver = 0
    logger.info("*******************************")
    logger.info("refs len: %s" %len(refs))
    logger.info("tars len: %s" %len(tars))
    logger.info("*******************************")
    if(len(tars) < len(refs)):
        temp = refs
        refs = tars
        tars = temp
    for ref in refs:
        logger.info("@@@@@@@@@@@@@@@@@@@__BEGIN__@@@@@@@@@@@@@@@@@@@@@")
        temp = []
        logger.info("ref:: %s" %ref)
        logger.info("fnl_refs length: %s" %len(fnl_refs))
        cms = ("__").join([ref.split("_")[2], ref.split("__")[1]])
        logger.info("cms1:: %s" %cms)
        for tar in tars:
            logger.info("tar:: %s" %tar)
            cms2 = ("__").join([tar.split("_")[2], tar.split("__")[1]])
            logger.info("cms2 %s" %cms2)
            if (cms == cms2):
                temp.append(tar)
                #tars.remove(tar)
            logger.info("temp length 160: %s" %len(temp))
        if (len(temp) > 1):
            logger.info('176 eilute')
            tmp2 = []
            for tmp in temp:
                cms1 = tmp.split("-")[2].split("__")[0]
                if (int(cms1[1:]) >= max_ver):
                    max_ver = int(cms1[1:])
                    tmp2.append(tmp)
            temp = tmp2
            logger.info("temp length:184 %s" %len(temp))
        if (len(temp) > 1):
            tmp2 = []
            for tmp in temp:
                logger.info("tmp: %s" %tmp)
                cms1 = tmp.split("_")[1]
                logger.info("cms1:: %s" %cms1)
                logger.info("max versija:: %s" %max_ver)
                logger.info("versijos dydis:: %s" %int(cms1[1:]))
                if (int(cms1[1:]) >= max_ver):
                    max_ver = int(cms1[1:])
                    logger.info("max versija pakilo iki:: %s" %max_ver)
                    tmp2.append(tmp)
            temp = tmp2
            logger.info("temp length 174: %s" %len(temp))
        if (len(temp) > 1):
            logger.info('186 eilute')
            tmp2 = []
            length = 99
            for tmp in temp:
                cms1 = ref.split("-")[1]
                logger.info("ref split: %s" %cms1)
                tmp_split = tmp.split("-")[1]
                logger.info("lengvisho lengtas: %s" %length)
                logger.info("tmp: %s" %tmp)
                logger.info("leng ilgis: %s" %(levenshtein(cms1, tmp_split)))
                if (levenshtein(cms1, tmp_split) <= length):
                    length = levenshtein(cms1, tmp_split)
                    logger.info("lengvisho lengtas gale: %s" %length)
                    tmp2 = []
                    tmp2.append(tmp)
            temp = tmp2
        if (len(temp) > 1):
            logger.info("**************************************")
            logger.info("Rado %s atitikmenis ;//" %len(temp))
            logger.info("**************************************")
            for x in temp:
                logger.info(x)
        #not finished. not fully implemented. left only calculate.
        if (len(temp) > 0):
            fnl_tars.append(temp[0])
            fnl_refs.append(ref)
        logger.info("@@@@@@@@@@@@@@@@@@@@_END_@@@@@@@@@@@@@@@@@@@@@@@@@@")
    return fnl_refs, fnl_tars


#'DQM_V0001_R000000001__RelValMinBias_13TeV_pythia8__CMSSW_8_0_1-80X_mcRun2_asymptotic_v6_gcc530_gen-v1__DQMIO.root'


def get_list_of_wf(refs, tars, unpaired, category):
    logger.info("***********************************************")
    path = os.path.join(local_relmon_request, category["name"])
    wf_list = os.listdir(path)
    samples = {}
    wf_list.remove("cookie.txt")
    unique_list = []
    for wf in wf_list:
        cms = wf.split("__")[2].split("-")[0]
        unique_list.append(cms)
        logger.info("%s /n" %wf)
    logger.info("print entire list::: %s" %unique_list)
    logger.info("***************************")
    unique_list = list(set(unique_list))
    # if(len(unique_list) == 1):

    for var in unique_list:
        samples[var] = [];
    for wf in wf_list:
        cms = wf.split("__")[2].split("-")[0]
        samples[cms].append(wf)
    logger.info("samples length: %s" %len(samples))
    if (len(samples) > 2):
        max1 = [k for k in samples.keys() if len(samples.get(k))==max([len(n) for n in samples.values()])]
        refs = samples[max1]
        del samples[max1]
        max2 = [k for k in samples.keys() if len(samples.get(k))==max([len(n) for n in samples.values()])]
        tars = samples[max2]
        del samples[max2]
    else:
        logger.info("sample0 %s" %(samples.keys()[0]))
        logger.info("sample1 %s" %(samples.keys()[1]))
        refs = samples[samples.keys()[0]]
        tars = samples[samples.keys()[1]]
    ref2 = refs
    tars2 = tars
    returned_lists = construct_final_lists(refs, tars)
    refs = returned_lists[0]
    tars = returned_lists[1]
    logger.info("ref1 ir ref2 ilgiai: %s--->%s" %(len(ref2), len(refs)))
    logger.info("tars1 ir tars2 ilgiai: %s--->%s" %(len(tars2), len(tars)))
    if (tars2 == tars):
        logger.info("tars and tars2 are equals")
    
    #Have only 2 lists of the biggest list. Others lists, with less variables are frozed.
    #for (ref in refs)
    return refs, tars, unpaired   

def validate(category_name, HLT):
  #  global logFile
    logger.info("atejoo i validate")
    local_subreport = get_local_subreport_path(category_name, HLT)
    # TODO: handle dirs creation failures
    if (not os.path.exists(local_subreport)):
        os.makedirs(local_subreport)
        os.chmod(local_subreport, 0775)
    logger.info("abspath %s" %os.path.abspath(category_name))
    logger.info("local_subreport::; %s" %os.path.abspath(local_subreport))
    tar_list = ["teststest"]
    ref_list = []
    unpaired_list = []
    logger.info("final refssss pries: %s" %ref_list)
    returned_lists = get_list_of_wf(ref_list, tar_list, unpaired_list, category)
    cat_path = category_name+"/"
    ref_list = [cat_path + s for s in returned_lists[0]]
    tar_list = [cat_path + s for s in returned_lists[1]]
    logger.info("final refssss: %s" %ref_list)
    logger.info("final tarssss: %s" %tar_list)

    rs = (",").join(ref_list)
    ts = (",").join(tar_list)

    validation_cmd = ["ValidationMatrix.py",
                      "-R", str(rs),
                      "-T", str(ts),

                      "-o", os.path.abspath(local_subreport),
                      "-N 6",
                      "--hash_name"]

        # validation_cmd = ["ValidationMatrix.py",
    #                   "-a", os.path.abspath(category_name),
    #                   "-o", os.path.abspath(local_subreport),
    #                   "-N 6",
    #                   "--hash_name"]

    # logger.info("print validation_cmd2: %s" %validation_cmd2)
    logger.info("working dir: %s" %os.getcwd())
    logger.info("print validation_cmd: %s" %validation_cmd)

    if (HLT):
        validation_cmd.append("--HLT")
    logFile.write("!       SUBPROCESS: " + " ".join(validation_cmd) + "\n")
    logger.info("! SUBPROCESS: %s" %(" ".join(validation_cmd)))
    logFile.flush()
    # return process exit code
    return subprocess.Popen(
        validation_cmd, stdout=logFile, stderr=logFile).wait()


def compress(category_name, HLT):
    logger.info("catg name::: %s ir hlt::: %s" %(category_name, HLT))
    local_subreport = get_local_subreport_path(category_name, HLT)
    logger.info("print local local_subreport: %s" %(local_subreport))
    dir2webdir_cmd = ['dir2webdir.py', local_subreport]
    logFile.write("!       SUBPROCESS:" + " ".join(dir2webdir_cmd) + "\n")
    logFile.flush()
    return (subprocess.Popen(dir2webdir_cmd,
                             stdout=logFile,
                             stderr=logFile)
            .wait())


def move_to_afs(category_name, HLT):
    logger.info("atejo 5 move_to_afs")
    local_subreport = get_local_subreport_path(category_name, HLT)
    remote_subreport = os.path.join(remote_reports,
                                    os.path.basename(local_subreport))
    # TODO: handle failures
    if (os.path.exists(remote_subreport)):
        shutil.rmtree(remote_subreport)
    shutil.copytree(local_subreport, remote_subreport)

for category in request.categories:
    ind = category["name"]
    logger.info("category::: %s" %ind)
    if (not category["lists"]["target"]):
        continue
    if (category["name"] == "Generator" or category["HLT"] != "only"):
        # validate and compress; failure if either task failed
        logger.info("i validate 200")
        logger.info("kodas: %s" %(validate(category["name"], False)))
        logger.info("kodas: %s" %(compress(category["name"], False)))
        if ((validate(category["name"], False) != 0) or
            (compress(category["name"], False) != 0)):
            # then:
            finalize_report_generation("failed")
            exit(1)
        move_to_afs(category["name"], False)
    if (category["name"] == "Generator"):
        continue
    if (category["HLT"] != "no"):
        logger.info("i validate 210")
        if ((validate(category["name"], True) != 0) or
            (compress(category["name"], True) != 0)):
            finalize_report_generation("failed")
            exit(1)
        move_to_afs(category["name"], True)
finalize_report_generation("finished")