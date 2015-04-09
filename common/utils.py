"""Helper functions for relmon request service."""

import logging
try:  # Python 2.7+
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass
import stat
import sys
import os
import re
import json
import httplib

try:
    import paramiko
except ImportError:
    pass

import config
from config import CONFIG


HYPERLINK_REGEX = re.compile(r"href=['\"]([-./\w]*)['\"]")

logger = logging.getLogger(__name__)
logger.addHandler(NullHandler())

credentials = {}
# TODO: handle failures
with open(CONFIG.CREDENTIALS_PATH) as cred_file:
    credentials = json.load(cred_file)


def init_validation_logs_dir():
    logger.info("Initializing validation logs directory")
    logsdir = os.path.join(
        os.path.dirname(os.path.abspath(sys.modules['__main__'].__file__)),
        "static",
        "validation_logs")
    if (not os.path.isdir(logsdir)):
        logger.debug("Creating new validation logs directory")
        os.makedirs(os.path.join(logsdir))
    os.chmod(logsdir, 0777)
    logger.info("Validation logs directory initialized")


def prepare_remote():
    logger.info("Preparing remote maschine")
    local_main_path = os.path.dirname(
        os.path.abspath(sys.modules['__main__'].__file__))
    logger.debug("Local __main__ path: " + local_main_path)
    config.setstring("SERVICE_WORKING_DIR", local_main_path)
    config.write()
    transport = paramiko.Transport((CONFIG.REMOTE_HOST, 22))
    transport.connect(username=credentials["user"],
                      password=credentials["pass"])
    logger.info("Connected to " + CONFIG.REMOTE_HOST)
    sftp = paramiko.SFTPClient.from_transport(transport)
    # remove files on remote maschine
    logger.info("Removing old files on remote machine")
    for fattr in sftp.listdir_attr(CONFIG.REMOTE_WORK_DIR):
        if (stat.S_ISREG(fattr.st_mode)):
            try:
                sftp.remove(os.path.join(CONFIG.REMOTE_WORK_DIR,
                                         fattr.filename))
            except IOError:
                pass
    # create remote "common" directory if it does not exist
    try:
        sftp.mkdir(os.path.join(CONFIG.REMOTE_WORK_DIR, "common"))
    except IOError:
        pass
    # remove files from remote "common" directory
    for fattr in sftp.listdir_attr(
            os.path.join(CONFIG.REMOTE_WORK_DIR, "common")):
        if (stat.S_ISREG(fattr.st_mode)):
            try:
                sftp.remove(os.path.join(
                    CONFIG.REMOTE_WORK_DIR, "common", fattr.filename))
            except IOError:
                pass
    # put files on remote maschine
    logger.info("Uploading files to remote maschine")
    sftp.put(os.path.join(local_main_path, "config.py"),
             os.path.join(CONFIG.REMOTE_WORK_DIR, "config.py"))
    sftp.put(os.path.join(local_main_path, "config"),
             os.path.join(CONFIG.REMOTE_WORK_DIR, "config"))
    for fname in os.listdir(os.path.join(local_main_path, "remote")):
        sftp.put(os.path.join(local_main_path, "remote", fname),
                 os.path.join(CONFIG.REMOTE_WORK_DIR, fname))
        sftp.chmod(os.path.join(CONFIG.REMOTE_WORK_DIR, fname), 0775)

    for fname in os.listdir(os.path.join(local_main_path, "common")):
        sftp.put(os.path.join(local_main_path, "common", fname),
                 os.path.join(CONFIG.REMOTE_WORK_DIR, "common", fname))
    sftp.close()
    transport.close()
    logger.info("Remote maschine prepared")


def httpget(host, url, port=80):
    logger.info("HTTP GET " + host + url)
    conn = httplib.HTTPConnection(host, port)
    conn.connect()
    conn.request("GET", url=url)
    response = conn.getresponse()
    status = response.status
    logger.debug("HTTP code " + str(status))
    content = response.read()
    conn.close()
    return status, content


def httpsget(host,
             url,
             port=443,
             certpath=CONFIG.CERTIFICATE_PATH,
             keypath=CONFIG.KEY_PATH,
             password=None):
    logger.info("HTTPS GET " + host + url)
    conn = httplib.HTTPSConnection(host=host,
                                   port=port,
                                   key_file=keypath,
                                   cert_file=certpath)
    conn.connect()
    conn.request("GET", url=url)
    response = conn.getresponse()
    status = response.status
    logger.debug("HTTP code " + str(status))
    content = response.read()
    conn.close()
    return status, content


def httpsget_large_file(filepath,
                        host,
                        url,
                        port=443,
                        certpath=CONFIG.CERTIFICATE_PATH,
                        keypath=CONFIG.KEY_PATH,
                        password=None):
    logger.info("HTTPS GET large file " + host + url +
                " to file '" + filepath + "'")
    conn = httplib.HTTPSConnection(host=host,
                                   port=port,
                                   key_file=keypath,
                                   cert_file=certpath)
    conn.connect()
    conn.request("GET", url=url)
    response = conn.getresponse()
    with open(filepath, "wb") as dest_file:
        while True:
            chunk = response.read(1048576)
            if chunk:
                dest_file.write(chunk)
                dest_file.flush()
            else:
                break
    conn.close()


def http(method, host, url, data=None, port=80):
    logger.info("HTTP " + method + " " + host + url)
    logger.debug(data)
    conn = httplib.HTTPConnection(host, port)
    conn.connect()
    headers = {"Content-type": "application/json"}
    conn.request(method=method,
                 url=url,
                 body=data,
                 headers=headers)
    response = conn.getresponse()
    status = response.status
    logger.debug("HTTP code " + str(status))
    content = response.read()
    conn.close()
    return status, content


# given workflow name, returns workflow status from Workload
# Management database 'wmstats'
def get_workload_manager_status(sample_name):
    url = CONFIG.WMSTATS_URL + "/_all_docs?keys=%5B%22"
    url += sample_name
    url += "%22%5D&include_docs=true"
    status, data = httpsget(host=CONFIG.CMSWEB_HOST,
                            url=url)
    # TODO: handle failures
    if (status != httplib.OK):
        return None
    try:
        data = json.loads(data)
        # FIXME: take status with most reecent 'update-time', instead
        # of taking last element
        wm_status = (
            data["rows"][0]["doc"]["request_status"][-1]["status"])
        return wm_status
    except (ValueError, LookupError):
        logger.exception("get_workload_manager_status returning with error")
        return None


def get_ROOT_file_urls(CMSSW, category_name):
    """
    str:CMSSW -- version . E.g. "CMSSW_7_2_"
    bool:MC -- True -> Monte Carlo, False -> data
    """
    url = CONFIG.DQM_ROOT_URL
    if (category_name == "Data"):
        url += "/RelValData/"
    else:
        url += "/RelVal/"
    CMSSW_parsed = re.search("CMSSW_\d_\d_", CMSSW)
    if (CMSSW_parsed is None):
        logger.warning("Failed parsing CMSSW version from '" + CMSSW + "'")
        return None
    url += CMSSW_parsed.group() + "x/"
    status, data = httpsget(host=CONFIG.CMSWEB_HOST,
                            url=url)
    if (status != httplib.OK):
        logger.warning("get_ROOT_file_urls failing with HTTP request")
        return None
    hyperlinks = HYPERLINK_REGEX.findall(data)[1:]
    for u_idx, url in enumerate(hyperlinks):
        hyperlinks[u_idx] = url
    return hyperlinks


def get_DQMIO_status(sample_name):
    """Given sample name returns DQMIO dataset status. If given sample
    produces DQMIO dataset then the second object of the returned
    tuple is the name of that DQMIO dataset
    """
    status, data = httpsget(host=CONFIG.CMSWEB_HOST,
                            url=CONFIG.DATATIER_CHECK_URL + '/' + sample_name)
    if (status == httplib.OK):
        if ("DQMIO" in data):
            rjson = json.loads(data.replace('\'', '\"'))
            DQMIO_string = [i for i in rjson if "DQMIO" in i][0]
            if ("None" in DQMIO_string):
                return ("waiting", None)
            return ("DQMIO", DQMIO_string)
        return ("NoDQMIO", None)
    logger.warning("get_DQMIO_status failing with HTTP request")
    return ("waiting", None)


def get_run_count(DQMIO_string):
    if (not DQMIO_string):
        return None
    status, data = httpsget(
        host=CONFIG.CMSWEB_HOST,
        url=CONFIG.DBSREADER_URL + "/runs?dataset=" + DQMIO_string)
    if (status != httplib.OK):
        logger.warning("get_run_count failing with HTTP request")
        return None
    try:
        rjson = json.loads(data)
        return (len(rjson[0]["run_num"]))
    except (ValueError, LookupError):
        logger.exception("get_workload_manager_status returning with error")
        return None


def get_ROOT_name_part(DQMIO_string):
    if (DQMIO_string):
        parts = DQMIO_string.split('/')[1:]
        DS = parts[0]
        CMSSW = parts[1].split('-')[0]
        PS = parts[1].split('-')[1]
        return DS + "__" + CMSSW + '-' + PS + '-'
