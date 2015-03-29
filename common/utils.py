"""
Helper functions for relmon request service.
"""
import re
import json
import httplib

# TODO: move hardcoded values to config file

CMSWEB_HOST = "cmsweb.cern.ch"
DQM_ROOT_URL = "/dqm/relval/data/browse/ROOT/"
DATATIER_CHECK_URL = "/reqmgr/reqMgr/outputDatasetsByRequestName/"
DATASET_RUNS_URL = "/dbs/prod/global/DBSReader/runs?dataset="
HYPERLINK_REGEX = re.compile(r"href=['\"]([-./\w]*)['\"]")
CERTIFICATE_PATH = "/afs/cern.ch/user/j/jdaugala/.globus/usercert.pem"
KEY_PATH = "/afs/cern.ch/user/j/jdaugala/.globus/userkey.pem"
CREDENTIALS_PATH = "/afs/cern.ch/user/j/jdaugala/private/credentials"
REMOTE_WORK_DIR = "/build/jdaugala/relmon"
REMOTE_HOST = "cmsdev04.cern.ch"
DOWNLOADER_CMD = "cd " + REMOTE_WORK_DIR + "; ./download_DQM_ROOT.py "
REPORT_GENERATOR_CMD = ("cd /build/jdaugala/CMSSW_7_4_0_pre8\n"
                        " eval `scramv1 runtime -sh`\n"
                        "cd " + REMOTE_WORK_DIR +
                        "\n ./compare.py ")
CLEANER_CMD = "cd " + REMOTE_WORK_DIR + "; ./clean.py "

credentials = {}
# TODO: handle failures
with open(CREDENTIALS_PATH) as cred_file:
    credentials = json.load(cred_file)


def httpget(host, url, port=80):
    conn = httplib.HTTPConnection(host, port)
    conn.connect()
    conn.request("GET", url=url)
    response = conn.getresponse()
    status = response.status
    content = response.read()
    conn.close()
    return status, content


def httpsget(host,
             url,
             port=443,
             certpath=CERTIFICATE_PATH,
             keypath=KEY_PATH,
             password=None):
    conn = httplib.HTTPSConnection(host=host,
                                   port=port,
                                   key_file=keypath,
                                   cert_file=certpath)
    conn.connect()
    conn.request("GET", url=url)
    response = conn.getresponse()
    status = response.status
    content = response.read()
    conn.close()
    return status, content


def httpsget_large_file(filepath,
                        host,
                        url,
                        port=443,
                        certpath=CERTIFICATE_PATH,
                        keypath=KEY_PATH,
                        password=None):
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
    conn = httplib.HTTPConnection(host, port)
    conn.connect()
    headers = {"Content-type": "application/json"}
    conn.request(method=method,
                 url=url,
                 body=data,
                 headers=headers)
    response = conn.getresponse()
    status = response.status
    content = response.read()
    conn.close()
    return status, content


# given workflow name, returns workflow status from Workload
# Management database 'wmstats'
def get_workload_manager_status(sample_name):
    url = "/couchdb/wmstats/_all_docs?keys=%5B%22"
    url += sample_name
    url += "%22%5D&include_docs=true"
    status, data = httpsget(host=CMSWEB_HOST,
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
    except (ValueError, LookupError) as err:
        print(err)
        return None


def get_ROOT_file_urls(CMSSW, category_name):
    """
    str:CMSSW -- version . E.g. "CMSSW_7_2_"
    bool:MC -- True -> Monte Carlo, False -> data
    """
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
    status, data = httpsget(host=CMSWEB_HOST,
                            url=url)
    # TODO: handle failure
    if (status != httplib.OK):
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
    status, data = httpsget(host=CMSWEB_HOST,
                            url=DATATIER_CHECK_URL + sample_name)
    # TODO: handle request failure
    if (status == httplib.OK):
        if ("DQMIO" in data):
            rjson = json.loads(data.replace('\'', '\"'))
            DQMIO_string = [i for i in rjson if "DQMIO" in i][0]
            if ("None" in DQMIO_string):
                return ("waiting", None)
            return ("DQMIO", DQMIO_string)
        return ("NoDQMIO", None)
    return ("waiting", None)


def get_run_count(DQMIO_string):
    if (not DQMIO_string):
        return None
    status, data = httpsget(host=CMSWEB_HOST,
                            url=DATASET_RUNS_URL + DQMIO_string)
    # TODO: handle request failure
    if (status != httplib.OK):
        return None
    try:
        rjson = json.loads(data)
        return (len(rjson[0]["run_num"]))
    except (ValueError, LookupError) as err:
        print(err)
        return None


def get_ROOT_name_part(DQMIO_string):
    if (DQMIO_string):
        parts = DQMIO_string.split('/')[1:]
        DS = parts[0]
        CMSSW = parts[1].split('-')[0]
        PS = parts[1].split('-')[1]
        return DS + "__" + CMSSW + '-' + PS + '-'


# def sample_fraction_by_status(relmon_request, status, ignore=None):
#     not_ignored = 0
#     with_status = 0
#     for sample in iters.samples(
#             relmon_request["categories"],
#             only_statuses=None,
#             skip_statuses=ignore):
#         if (sample["status"] in status):
#             with_status += 1
#         not_ignored += 1
#     if (not_ignored == 0):
#         return fractions.Fraction(0)
#     return fractions.Fraction(with_status, not_ignored)


# # NOTE: exec_command is non blocking, it is probably not needed to
# # wrap it in another thread - SSHThread
# class SSHThread(threading.Thread):
#     def __init__(self, command):
#         import paramiko
#         threading.Thread.__init__(self)
#         self.command = command
#         self.ssh_client = paramiko.SSHClient()
#         self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

#     def run(self):
#         print("SSHThread")
#         print(self.command)
#         self.ssh_client.connect(REMOTE_HOST,
#                                 username=credentials["user"],
#                                 password=credentials["pass"])
#         (stdin, stdout, stderr) = self.ssh_client.exec_command(self.command)
#         print(stdout.readlines())
#         print(stderr.readlines())
#         self.ssh_client.close()

#     def stopSSH(self):
#         self.ssh_client.close()


# class TerminatorThread(threading.Thread):
#     """Documentation for TerminatorThread
#     """
#     def __init__(self, request_id):
#         super(TerminatorThread, self).__init__()
#         self.request_id = request_id

#     def _remove_from_data(self):
#         with relmon_shared.data_lock:
#             for i in xrange(len(relmon_shared.data)):
#                 if relmon_shared.data[i]["id"] == self.request_id:
#                     relmon_shared.data.pop(i)
#                     break
#             relmon_shared.write_data()

#     def run(self):
#         if (is_downloader_alive(self.request_id)):
#             downloader = relmon_shared.downloaders[self.request_id]
#             downloader.stopSSH()
#             downloader.join()
#         if (is_reporter_alive(self.request_id)):
#             reporter = relmon_shared.reporters[self.request_id]
#             reporter.stopSSH()
#             reporter.join()
#         cleaner = SSHThread(CLEANER_CMD + str(self.request_id))
#         cleaner.start()
#         cleaner.join()
#         if (os.path.exists("static/validation_logs/" +
#                            str(self.request_id) + ".log")):
#             os.remove("static/validation_logs/" +
#                       str(self.request_id) + ".log")
#         self._remove_from_data()


# def is_downloader_alive(request_id):
#     return (request_id in relmon_shared.downloaders and
#             relmon_shared.downloaders[request_id].isAlive())


# def is_reporter_alive(request_id):
#     return (request_id in relmon_shared.reporters and
#             relmon_shared.reporters[request_id].isAlive())


# def is_terminator_alive(request_id):
#     return (request_id in relmon_shared.terminators and
#             relmon_shared.terminators[request_id].isAlive())


# def start_downloader(request_id):
#     if (is_downloader_alive(request_id)):
#         return None
#     downloader = SSHThread(
#         DOWNLOADER_CMD + str(request_id))
#     relmon_shared.downloaders[request_id] = downloader
#     downloader.start()
#     return downloader


# def start_reporter(request_id):
#     if (is_reporter_alive(request_id)):
#         return None
#     reporter = SSHThread(
#         REPORT_GENERATOR_CMD + str(request_id))
#     relmon_shared.reporters[request_id] = reporter
#     reporter.start()
#     return reporter


# def start_terminator(request_id):
#     if (is_terminator_alive(request_id)):
#         return None
#     terminator = TerminatorThread(request_id)
#     relmon_shared.terminators[request_id] = terminator
#     terminator.start()
#     return terminator
