"""RelMon request (campaign) manipulation tools"""

import logging
try:  # Python 2.7+
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass
import fractions
import Queue
import threading
import time
import json
import itertools

try:
    import paramiko
except ImportError:
    pass

from config import CONFIG
from common import utils


logger = logging.getLogger(__name__)
logger.addHandler(NullHandler())

credentials = {}
try:
    logger.info("Reading credentials")
    with open(CONFIG.CREDENTIALS_PATH) as cred_file:
        logger.info("Parsing credentials")
        credentials = json.load(cred_file)
    credentials["username"]
    credentials["password"]
except:
    logger.exception("Failed credentials reading/parsing")
    raise


class RelmonRequest():
    """Documentation for RelmonRequest

    """
    def __init__(self, name, threshold, categories,
                 id_=None, status="initial", log=False):
        self.id_ = id_
        if (self.id_ is None):
            self.id_ = int(time.time())
            logger.debug("Initializing new RelmonRequest " + str(self.id_))
        else:
            logger.debug("Initializing RelmonRequest " + str(self.id_))
        if (int(threshold) < 0 or int(threshold) > 100):
            logger.warning("RelmonRequest " + str(self.id_) +
                           "threshold out of range")
            raise ValueError("Threshold must be an integer [0;100]")
        self.lock = threading.RLock()
        self.high_priority_queue = Queue.Queue()
        self.name = name
        self.threshold = threshold
        self.categories = categories
        self.status = status
        self.log = log
        for category in self.categories:
            for sample_list in category["lists"].itervalues():
                for sample_idx, sample in enumerate(sample_list):
                    if (type(sample) is not dict):
                        tmp_sample = {"name": sample,
                                      "status": "initial",
                                      "wm_status": ""}
                        sample_list[sample_idx] = tmp_sample

    def to_dict(self):
        logger.debug(str(self.id_) + "to_dict()")
        return {
            "id_": self.id_,
            "name": self.name,
            "status": self.status,
            "threshold": self.threshold,
            "log": self.log,
            "categories": self.categories
        }

    def get_access(self):
        logger.debug("Accessing RR " + str(self.id_))
        self.high_priority_queue.join()
        # commented because somewhere it just stuck
        # self.lock.acquire()
        logger.debug("Got access to RR " + str(self.id_))

    def release_access(self):
        # commented because somewhere it just stuck
        # self.lock.release()
        logger.debug("Released access to RR " + str(self.id_))

    def get_priority_access(self):
        logger.debug("Priority accessing RR " + str(self.id_) +
                     " lock: " + str(self.lock))
        self.high_priority_queue.put(self)
        self.lock.acquire()
        logger.debug("Got priority access to RR " + str(self.id_))

    def release_priority_access(self):
        self.lock.release()
        self.high_priority_queue.get()
        self.high_priority_queue.task_done()
        logger.debug("Released priority access to RR " + str(self.id_))

    def is_download_ready(self):
        frac_ROOT = self.sample_fraction(
            ["ROOT", "downloaded"], ["NoDQMIO", "NoROOT", "failed_rqmgr"])
        frac_threshold = fractions.Fraction( int(self.threshold), 100)
        return frac_ROOT >= frac_threshold

    def is_ROOT_100(self):
        frac_ROOT = self.sample_fraction(["ROOT"], ["NoDQMIO", "NoROOT", "failed_rqmgr"])
        return frac_ROOT == fractions.Fraction(1, 1)

    def has_ROOT(self):
        frac_ROOT = self.sample_fraction(["ROOT"], ["NoDQMIO", "NoROOT", "failed_rqmgr"])
        return frac_ROOT > fractions.Fraction(0)

    def sample_fraction(self, status, ignore=None):
        not_ignored = 0
        with_status = 0
        for sample in self.samples_iter(
                only_statuses=None,
                skip_statuses=ignore):
            if (sample["status"] in status):
                with_status += 1
            not_ignored += 1
        if (not_ignored == 0):
            return fractions.Fraction(0)
        return fractions.Fraction(with_status, not_ignored)

    def samples_iter(self, only_statuses=None, skip_statuses=None):
        for category in self.categories:
            for sample_list in category["lists"].itervalues():
                for sample in sample_list:
                    if (skip_statuses is not None and
                        sample["status"] in skip_statuses):
                        # then:
                        continue
                    if (only_statuses is not None):
                        if (sample["status"] in only_statuses):
                            yield sample
                        continue
                    yield sample


class Worker(threading.Thread):
    """Documentation for Worker

    """
    def __init__(self):
        super(Worker, self).__init__()

    def stop(self):
        raise NotImplementedError("Workers should implement 'stop' method.")


class StatusUpdater(Worker):
    """Documentation for StatusUpdater

    """
    def __init__(self, request):
        super(StatusUpdater, self).__init__()
        self.request = request
        self._stop = False

    def run(self):
        """Update sample statuses and return count of samples that changed
        their statuses to 'ROOT' by this update"""
        logger.info("Runing StatusUpdater for RR " + str(self.request.id_))
        try:
            has_new_DQMIO = self.update_DQMIO_statuses()
            if (has_new_DQMIO):
                self.update_ROOT_names_parts()
                self.update_run_counts()
            self.update_wm_statuses()
            self.update_ROOT_statuses()
        except:
            logger.exception("Uncaught exception in StatusUpdater run")
            raise
        finally:
            logger.info("Finished StatusUpdater")

    def update_DQMIO_statuses(self):
        request_has_new_DQMIO_samples = False
        for sample in self.request.samples_iter(["initial", "waiting"]):
            if (self._stop):
                return
            DQMIO_status, DQMIO_string = utils.get_DQMIO_status(sample["name"])
            if (self._stop):
                return
            self.request.get_access()
            # NOTE: assuming SAMPLE status has not changed (it
            # shouldn't have) in other thread while checking DQMIO
            # status
            sample["status"] = DQMIO_status
            sample["DQMIO_string"] = DQMIO_string
            self.request.release_access()
            if (sample["status"] == "DQMIO"):
                request_has_new_DQMIO_samples = True
        return request_has_new_DQMIO_samples

    def update_ROOT_names_parts(self):
        self.request.get_access()
        for sample in self.request.samples_iter(["DQMIO"]):
            if (self._stop):
                self.request.release_access()
                return
            sample["ROOT_file_name_part"] = (
                utils.get_ROOT_name_part(sample["DQMIO_string"]))
        self.request.release_access()

    def update_run_counts(self):
        for sample in self.request.samples_iter(["DQMIO"]):
            if (self._stop):
                return
            run_count = utils.get_run_count(sample["DQMIO_string"])
            if (self._stop):
                return
            self.request.get_access()
            sample["run_count"] = run_count
            self.request.release_access()

    def update_wm_statuses(self):
        """Update sample statuses from Workload Manager"""
        for sample in self.request.samples_iter(["initial",
                                                 "waiting",
                                                 "DQMIO"], ["wf doesn't exist"]):
            #elapsed_time = (int(time.time()) - self.request.id_) / 60
            #if ((elapsed_time >= 9) and (sample["status"]=="waiting")):
            #    sample["wm_status"] = "wf doesn't exist"
            #    continue
            if (self._stop):
                return
            wm_status = (
                utils.get_workload_manager_status(sample["name"]))
            if (sample["wm_status"] == wm_status):
                continue
            if (self._stop):
                return
            self.request.get_access()
            sample["wm_status"] = wm_status
            self.request.release_access()

    # TODO: reduce indentation
    def update_ROOT_statuses(self):
        categories_and_listnames = itertools.product(
            self.request.categories,
            ["target", "reference"])
        # Temporary commented. Explanation @ 302 line
        statusShouldBeFailed = True
        for (category, list_name) in categories_and_listnames:
            sample_list = category["lists"][list_name]
            if (not sample_list):
                continue
            if (self._stop):
                return
            for sample_index in range(len(sample_list)):
                file_urls = utils.get_ROOT_file_urls(
                    sample_list[sample_index]["name"],
                    category["name"])
                #if (not file_urls):
                    # FIXME: temporary solution
                self.request.get_access()
                if (sample_list[sample_index]["wm_status"] == "wf doesn't exist"):
                    sample_list[sample_index]["status"] = "failed_rqmgr"
                    # Temporary commented. Explanation @ 302 line
                    # else:
                    #     sample_list[sample_index]["status"] = "failed"
                if (sample_list[sample_index]["status"] != "failed_rqmgr"):
                    statusShouldBeFailed = False
                if (self._stop):
                    self.request.release_access()
                    return
                for sample in sample_list:
                    if ((sample["status"] != "DQMIO")):
                        continue

                    if (file_urls):
                        matches = [u for u in file_urls
                                   if (sample["ROOT_file_name_part"] in u)]
                        self.request.get_access()
                        sample["root_count"] = len(matches)
                        if (len(matches) > 0 and
                            len(matches) >= sample["run_count"]):
                            # then:
                            sample["status"] = "ROOT"
                        elif (CONFIG.IGNORE_NOROOT_WORKFLOWS and
                                sample["wm_status"] in CONFIG.FINAL_WM_STATUSES):
                            sample["status"] = "NoROOT"
                            logger.info(sample["name"] + "setting to 'NoROOT', " +
                                        "wm_status is final but not enough " +
                                        "files were found with '" +
                                        sample["ROOT_file_name_part"] + "'")
                        self.request.release_access()
        #I think that this part not important anymore. Test with another task.
        if (statusShouldBeFailed):
           self.request.get_access()
           self.request.status = "failed"
           self.request.release_access()


    def stop(self):
        self._stop = True
        logger.info("Stopping StatusUpdater for RR " + str(self.request.id_))

class SSHWorker(Worker):
    def __init__(self, command):
        super(SSHWorker, self).__init__()
        self.command = command
        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ret_code = None

    def run(self):
        logger.info("SSHWorker run " + self.command)
        try:
            self.ssh_client.connect(CONFIG.REMOTE_HOST,
                                    username=credentials["username"],
                                    password=credentials["password"])
            # NOTE exec_command timeout does not work. Think of something..
            (_, stdout, stderr) = self.ssh_client.exec_command(self.command)
            self.ret_code = stdout.channel.recv_exit_status()
            if (self.ret_code != 0):
                logger.error("Remote command '" + self.command +
                             "' returned with code " + str(self.ret_code) +
                             ". More info might be found in log files at " +
                             CONFIG.REMOTE_HOST + ':' + CONFIG.REMOTE_WORK_DIR)
            self.ssh_client.close()
        except:
            logger.exception("Uncaught exception in SSHWorker run")
            raise
        finally:
            logger.info("Finished SSHWorker")

    def stop(self):
        self.ssh_client.close()
        logger.info("Stopping SSHWorker")


class Downloader(SSHWorker):
    """Documentation for Downloader

    """
    def __init__(self, request):
        super(Downloader, self).__init__(
            "cd " + CONFIG.REMOTE_WORK_DIR + ';' +
            "./download_ROOT.py " + str(request.id_) +
            " > download_ROOT.out 2>&1")


class Reporter(SSHWorker):
    """Documentation for Reporter

    """
    def __init__(self, request):
        super(Reporter, self).__init__(
            "cd " + CONFIG.REMOTE_CMSSW_DIR + ';' +
            "eval `scramv1 runtime -sh`" +
            "cd " + CONFIG.REMOTE_WORK_DIR + ';' +
            "./compare.py " + str(request.id_) + " > compare.out 2>&1")


class Cleaner(SSHWorker):
    """Documentation for Cleaner

    """
    def __init__(self, request):
        super(Cleaner, self).__init__(
            "cd " + CONFIG.REMOTE_WORK_DIR + ';' +
            "./clean.py " + str(request.id_) + " > clean.out 2>&1")
