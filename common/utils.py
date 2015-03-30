"""
Helper functions for relmon request service.
"""
import re
import json
import httplib
import config as CONFIG

HYPERLINK_REGEX = re.compile(r"href=['\"]([-./\w]*)['\"]")

credentials = {}
# TODO: handle failures
with open(CONFIG.CREDENTIALS_PATH) as cred_file:
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
             certpath=CONFIG.CERTIFICATE_PATH,
             keypath=CONFIG.KEY_PATH,
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
                        certpath=CONFIG.CERTIFICATE_PATH,
                        keypath=CONFIG.KEY_PATH,
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
    except (ValueError, LookupError) as err:
        print(err)
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
    CMSSW = re.search("CMSSW_\d_\d_", CMSSW)
    # TODO: handle this error
    if (CMSSW is None):
        return None
    url += CMSSW.group() + "x/"
    status, data = httpsget(host=CONFIG.CMSWEB_HOST,
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
    status, data = httpsget(host=CONFIG.CMSWEB_HOST,
                            url=CONFIG.DATATIER_CHECK_URL + '/' + sample_name)
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
    status, data = httpsget(
        host=CONFIG.CMSWEB_HOST,
        url=CONFIG.DBSREADER_URL + "/runs?dataset=" + DQMIO_string)
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
