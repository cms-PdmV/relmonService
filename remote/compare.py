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
import glob
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
cookie = None
cookie = utils.get_sso_cookie(CONFIG.SERVICE_HOST)
if (cookie is None):
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
if (os.path.isdir(local_reports)):
    shutil.rmtree(local_reports)

logger.info("local_reports::: %s " %local_reports)

# TODO: handle failures
logFile = open(str(request.id_) + ".log", "w")
os.chmod(str(request.id_) + ".log", 0664)

remote_reports = os.path.join(CONFIG.RELMON_PATH, request.name)
if (os.path.isdir(remote_reports)):
    shutil.rmtree(remote_reports)
logger.info("remote_reports:: %s" %remote_reports)
def upload_log():
    global request
    cookie = None
    cookie = utils.get_sso_cookie(CONFIG.SERVICE_HOST)
    if (cookie is None):
        cookie = utils.get_sso_cookie(CONFIG.SERVICE_HOST)
        if (cookie is None):
            logger.error("Failed getting sso cookies for " + CONFIG.SERVICE_HOST)
            exit(1)
    status, data = utils.https(
        "POST",
        CONFIG.SERVICE_HOST,
        CONFIG.SERVICE_BASE + "/requests/" + str(request.id_) + ".log",
        data=open(str(request.id_) + ".log", "rb"),
        headers={"Cookie": cookie})
    if (status != httplib.OK):
        # FIXME: solve this problem
        print("PUT log fail")


# TODO: think of other ways for controllers to know about failures/success
def put_status(status):
    global logFile, request
    cookie = None
    cookie = utils.get_sso_cookie(CONFIG.SERVICE_HOST)
    if (cookie is None):
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
    ret = shutil.rmtree(local_relmon_request)
    logger.debug("rm dir:%s returned: %s" %(local_relmon_request, ret))


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


def get_downloaded_files_list(givenList, wf_list):
    logger.info("*******************************")
    logger.info("get_downloaded_files_list")
    logger.info("list length: %s" %len(givenList))
    logger.info("*******************************")
    final_list = []
    #if (len(givenList) > 1):
    for el in givenList:
        logger.info("print ef and get everything. \n%s" %el)
        pVar = []
        for wf in wf_list:
            wf_name = (("-").join(("__").join(wf.split("__")[1:3]).split("-")[:-1])) + "-"
            #logger.info("wf_name: %s" %wf_name)
            if (wf_name == el["ROOT_file_name_part"]):
            #if (wf_name == el[]):
                pVar.append(wf)
            #logger.info("wf with same name numb.: %s" %len(pVar))
        # if (el["root_count"] > 1 and el["run_count"] > 1 and len(pVar) > 1):
            #Filter different RUN_Numbers
        
        logger.info("el: %s viso rado: %s" %(el["ROOT_file_name_part"], len(pVar)))
        unique_list = []
        for p in pVar:
            unique_list.append(p.split("__")[0].split("_")[-1])
        unique_list = list(set(unique_list))
        logger.info("unique_list numb: %s" %len(unique_list))
        for p in unique_list:
            logger.info("print first from unique_list: %s" %p)
            temp = []
            for q in pVar:
                if (p == q.split("__")[0].split("_")[-1]):
                    temp.append(q)
            logger.info("temp lenght: %s with this uniq: %s" %(len(temp), q))
            if (len(temp) > 1):
                tmp2 = []
                max_ver = 0
                for tmp in temp:
                    logger.info("max version: %s" %max_ver)
                    logger.info("To check max Version: \n%s" %tmp)
                    if (int((tmp.split("__")[2].split("-")[-1])[1:]) > max_ver):
                        max_ver = int((tmp.split("__")[2].split("-")[-1])[1:])
                        tmp2 = []
                        tmp2.append(tmp)
                    elif(int((tmp.split("__")[2].split("-")[-1])[1:]) == max_ver):
                        tmp2.append(tmp)
                temp = tmp2
            #Find the other parameter the biggest version ever
            logger.info("temp lenght v2: %s" %len(temp))
            if (len(temp) > 1):
                tmp2 = []
                max_ver = 0
                for tmp in temp:
                    logger.info("max version: %s" %max_ver)
                    logger.info("to check second version: \n%s" %tmp)
                    if (int((tmp.split("_")[1])[1:]) > max_ver):
                        max_ver = int((tmp.split("_")[1])[1:])
                        tmp2.append(tmp)
                    elif(int((tmp.split("_")[1])[1:]) == max_ver):
                        tmp2.append(tmp)
                temp = tmp2
            logger.info("temp in the end lenght: %s" %len(temp))
            if(len(temp) > 1):
                logger.info("!!!!!!!!WARNING!!!!!!!!")
                logger.info("!!!!!!!!WARNING!!!!!!!!")
                logger.info("Something went wrong, there are more than one donwloaded file in the end")
                logger.info("!!!!!!!!WARNING!!!!!!!!")
                logger.info("!!!!!!!!WARNING!!!!!!!!")
            else: 
                final_list.append(temp[0])
    return final_list

def deleteCrashedFiles(refs, tars):
    logger.info("***********************************************")
    logger.info("deleteCrashedFiles")
    logger.info("pTar lenght: %s" %len(refs))
    logger.info("pRef lenght: %s" %len(tars))
    temp_ref = []
    temp_tar = tars
    for r in refs:
        logger.info("status %s" %r["status"])
        logger.info("1 " + str((r["status"] == "downloaded")))

        if ((r["status"] == "downloaded") or (r["status"] == "failed download")):
            if (r["run_count"] > 0):
                temp_ref.append(r)
        else:
            if ((r["status"] == "NoDQMIO") or (r["status"] == "failed_rqmgr")):
                continue
            logger.info("NoROOT or NoDQMIO or failed_rqmgr")
            for t in tars:
                if ((t["status"] != "NoDQMIO") and (t["status"] !="failed_rqmgr")):
                    rn = r["ROOT_file_name_part"]
                    tn = t["ROOT_file_name_part"]
                    if ((rn.split("__")[0] == tn.split("__")[0])
                        and (rn.split("__")[1].split("-")[1] == tn.split("__")[1].split("-")[1])):
                        temp_tar[:] = [d for d in temp_tar if d.get('ROOT_file_name_part') != tn]
    tars = []
    refs = temp_ref    
    for t in temp_tar:
        if ((t["status"] == "downloaded") or (t["status"] == "failed download")):
            if (t["run_count"] > 0):
                tars.append(t)
        else:
            if ((t["status"] == "NoDQMIO") or (t["status"] =="failed_rqmgr")):
                continue
            logger.info("NoROOT or NoDQMIO or failed_rqmgr")
            for r in temp_ref:
                rn = r["ROOT_file_name_part"]
                tn = t["ROOT_file_name_part"]
                if ((rn.split("__")[0] == tn.split("__")[0])
                    and (rn.split("__")[1].split("-")[1] == tn.split("__")[1].split("-")[1])):
                    refs[:] = [d for d in refs if d.get('ROOT_file_name_part') != rn] 
    return refs, tars


    

def get_list_of_wf(refs, tars, category):
    logger.info("***********************************************")
    logger.info("get_list_of_wf")
    ref2 = []
    tar2 = []
    changed = False
    old_ctg = os.getcwd()
    path = os.path.join(local_relmon_request, category["name"])
    os.chdir(path)
    wf_list = glob.glob("*.root")
    os.chdir(old_ctg)

    samples = {}
    logger.info("size of REF: %s" %len(refs))
    logger.info("size of Tar: %s" %len(tars))
    cleaned_lists = deleteCrashedFiles(refs, tars)
    refs = cleaned_lists[0]
    tars = cleaned_lists[1]
    logger.info("Cleaned lists sizes:")
    logger.info("ref size: %s" %len(refs))
    logger.info("tar size: %s" %len(tars))
    if (len(tars) > len(refs)):
        logger.info("sukeiciam listus vietomis")
        temp = refs
        refs = tars
        tars = refs
        changed = True

    for ref in refs:
        if ((ref["status"] == "NoDQMIO") or (ref["status"] == "NoROOT")):
            logger.info("NoROOT or NoDQMIO") 
        pRef = []
        pTar = []
        for tar in tars:
            if ((tar["status"] == "NoDQMIO") or (tar["status"] == "NoROOT")):
                logger.info("pyst ir continue   ")
                continue
            lref = ref["ROOT_file_name_part"]
            ltar = tar["ROOT_file_name_part"]
            if(ref["ROOT_file_name_part"].split("__")[0] == tar["ROOT_file_name_part"].split("__")[0]):
                lref = ref["ROOT_file_name_part"].split("-")[1].split("_")[-1]
                ltar = tar["ROOT_file_name_part"].split("-")[1].split("_")[-1]
                if((category["name"] == "Data") and (lref != ltar)):
                    continue
                pTar.append(tar)
            if (len(pTar) > 1):
                logger.info("WARNING")
                logger.info("RADO %s, I guess we need lengvish" %len(pTar))
                logger.info("WARNING")
                length = 20
                tar3 = []
                for p in pTar:
                    logger.info("---------------------------")
                    logger.info("%s" %ref["ROOT_file_name_part"])
                    logger.info("%s" %p["ROOT_file_name_part"])
                    logger.info("---------------------------")
                    lref = ref["ROOT_file_name_part"].split("__")[1].split("-")[1]
                    ltar = p["ROOT_file_name_part"].split("__")[1].split("-")[1]
                    logger.info("%s \n%s \nlength: %s\n**************" %(lref, ltar, levenshtein(lref, ltar)))
                    if(levenshtein(lref, ltar) < length):
                        tar3 = []
                        length = levenshtein(ltar, lref)
                        tar3.append(p)
                    elif (length == levenshtein(lref, ltar)):
                        tar3.append(p)
                pTar = tar3
                logger.info("pTar length: %s" %len(pTar))
        if (len(pTar) > 1):
            logger.info("WARNING")
            logger.info("WARNING")
            logger.info("I DO NOT KNOW WHAT TO DO :(((( ")
            logger.info("WANING")
            logger.info("WANING")
            logger.info("WANING")
            logger.info("pTar has more than one element ://")
        elif(len(pTar) < 1):
            logger.info("pTar has less than one element ://")
        elif(len(pTar) == 1):
            logger.info("pTar has one element://")
            logger.info("ref: %s" %ref["ROOT_file_name_part"])
            logger.info("tar: %s" %pTar[0]["ROOT_file_name_part"])
            ref2.append(ref)
            tar2.append(pTar[0])

    logger.info("pTar lenght: %s" %len(ref2))
    logger.info("pRef lenght: %s" %len(tar2))

    if (ref2 > 0 and tar2 > 0 and len(tar2)==len(ref2)):
        logger.info("REFFFFF")
        ref2 = get_downloaded_files_list(ref2, wf_list)
        logger.info("TARRRRR")
        tar2 = get_downloaded_files_list(tar2, wf_list)
        logger.info("print REF:")
        for x in ref2:
            logger.info("%s\n" %x)
        logger.info("print TAR:")
        for x in tar2:
            logger.info("%s\n" %x)
    if (changed):
        logger.info("change back lists")
        return tar2, ref2
    else:
        return ref2, tar2
       

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
    tar_list = category["lists"]["target"]
    ref_list = category["lists"]["reference"]
    logger.info("final refssss pries: %s" %ref_list)
    returned_lists = get_list_of_wf(ref_list, tar_list, category)
    cat_path = category_name+"/"

    logger.info("returned list::::\n%s" + str(returned_lists))
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

    logger.info("working dir: %s" %os.getcwd())
    logger.info("print validation_cmd: %s" %validation_cmd)

    if (HLT):
        validation_cmd.append("--HLT")
    logFile.write("!       SUBPROCESS: " + " ".join(validation_cmd) + "\n")
    logFile.flush()
    # return process exit code
    return subprocess.Popen(
        validation_cmd, stdout=logFile, stderr=logFile).wait()


def compress(category_name, HLT):
    local_subreport = get_local_subreport_path(category_name, HLT)
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
    if (not category["lists"]["target"]):
        continue
    if (category["name"] == "Generator" or category["HLT"] != "only"):
        # validate and compress; failure if either task failed
        if ((validate(category["name"], False) != 0) or
            (compress(category["name"], False) != 0)):
            # then:
            finalize_report_generation("failed")
            exit(1)
        move_to_afs(category["name"], False)
    if (category["name"] == "Generator"):
        continue
    if (category["HLT"] != "no"):
        if ((validate(category["name"], True) != 0) or
            (compress(category["name"], True) != 0)):
            finalize_report_generation("failed")
            exit(1)
        move_to_afs(category["name"], True)
finalize_report_generation("finished")