SERVICE_HOST = "188.184.185.27"
SERVICE_PORT = 80
REMOTE_WORK_DIR = "/build/jdaugala/relmon"
USER = "jdaugala"
REMOTE_USER = "jdaugala"
IGNORE_NOROOT_WORKFLOWS = True
DATA_FILE_NAME = "data"
POJECT_FILES_ONILINE_URL = (
    "https://raw.githubusercontent.com/cms-PdmV/relmonService/master")
LOG_DIR_AT_SERVICE = (
    "/home/relmon/relmon_request_service/static/validation_logs")
RELMON_PATH = (
    "/afs/cern.ch/cms/offline/dqm/ReleaseMonitoring-TEST/jdaugalaSandbox")
FINAL_WM_STATUSES = [
    "failed",
    "closed-out",
    "rejected",
    "rejected-archived",
    "aborted-completed",
    "aborted-archived",
    "announced",
    "normal-archived"]
FINAL_RELMON_STATUSES = [
    "failed",
    "terminating",
    "finished"]
REMOTE_HOST = "cmsdev04.cern.ch"
CREDENTIALS_PATH = "/afs/cern.ch/user/j/jdaugala/private/credentials"
REMOTE_CMSSW_DIR = "/build/jdaugala/CMSSW_7_4_0_pre8"
CERTIFICATE_PATH = "/afs/cern.ch/user/j/jdaugala/.globus/usercert.pem"
KEY_PATH = "/afs/cern.ch/user/j/jdaugala/.globus/userkey.pem"
CMSWEB_HOST = "cmsweb.cern.ch"
DQM_ROOT_URL = "/dqm/relval/data/browse/ROOT"
DATATIER_CHECK_URL = "/reqmgr/reqMgr/outputDatasetsByRequestName"
DBSREADER_URL = "/dbs/prod/global/DBSReader"
WMSTATS_URL = "/couchdb/wmstats"
TIME_AFTER_THRESHOLD_REACHED = 1
TIME_BETWEEN_STATUS_UPDATES = 180
TIME_BETWEEN_DOWNLOADS = 600
