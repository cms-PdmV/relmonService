"""RelMon request (campaign) manipulation tools"""

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

import config as CONFIG
from common import utils


credentials = {}
# TODO: handle failures
with open(CONFIG.CREDENTIALS_PATH) as cred_file:
    credentials = json.load(cred_file)


class RelmonRequest():
    """Documentation for RelmonRequest

    """
    def __init__(self, name, threshold, categories,
                 id_=None, status="initial", log=False):
        if (int(threshold) < 0 or int(threshold) > 100):
            raise ValueError("Threshold must be an integer [0;100]")
        self.lock = threading.RLock()
        self.high_priority_queue = Queue.Queue()
        self.name = name
        self.threshold = threshold
        self.categories = categories
        self.id_ = id_
        self.status = status
        self.log = log
        if (self.id_ is None):
            self.id_ = int(time.time())
        for category in self.categories:
            for sample_list in category["lists"].itervalues():
                for sample_idx, sample in enumerate(sample_list):
                    if (type(sample) is not dict):
                        tmp_sample = {"name": sample,
                                      "status": "initial",
                                      "wm_status": ""}
                        sample_list[sample_idx] = tmp_sample

    def to_dict(self):
        return {
            "id_": self.id_,
            "name": self.name,
            "status": self.status,
            "threshold": self.threshold,
            "log": self.log,
            "categories": self.categories
        }

    def get_access(self):
        self.high_priority_queue.join()
        self.lock.acquire()

    def release_access(self):
        self.lock.release()

    def get_priority_access(self):
        self.high_priority_queue.put(self)
        self.lock.acquire()

    def release_priority_access(self):
        self.lock.release()
        self.high_priority_queue.get()
        self.high_priority_queue.task_done()

    def is_download_ready(self):
        frac_ROOT = self.sample_fraction(
            ["ROOT", "downloaded"], ["NoDQMIO", "NoROOT"])
        frac_threshold = fractions.Fraction(self.threshold, 100)
        return frac_ROOT >= frac_threshold

    def is_ROOT_100(self):
        frac_ROOT = self.sample_fraction(["ROOT"], ["NoDQMIO", "NoROOT"])
        return frac_ROOT == fractions.Fraction(1, 1)

    def has_ROOT(self):
        frac_ROOT = self.sample_fraction(["ROOT"], ["NoDQMIO", "NoROOT"])
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
        has_new_DQMIO = self.update_DQMIO_statuses()
        if (has_new_DQMIO):
            self.update_ROOT_names_parts()
            self.update_run_counts()
        self.update_wm_statuses()
        self.update_ROOT_statuses()

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
                                                 "DQMIO"]):
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
        for (category, list_name) in categories_and_listnames:
            sample_list = category["lists"][list_name]
            if (not sample_list):
                continue
            if (self._stop):
                return
            file_urls = utils.get_ROOT_file_urls(
                sample_list[0]["name"],
                category["name"])
            if (not file_urls):
                # FIXME: temporary solution
                self.request.get_access()
                sample_list[0]["status"] = "failed"
                if (self.request.status not in
                    CONFIG.FINAL_RELMON_STATUSES):
                    # then:
                    self.request.status = "failed"
                self.request.release_access()
                # TODO: clean up
                return
            if (self._stop):
                return
            for sample in sample_list:
                if (sample["status"] != "DQMIO"):
                    continue
                matches = [u for u in file_urls
                           if (sample["ROOT_file_name_part"] in u)]
                self.request.get_access()
                sample["root_count"] = len(matches)
                if (len(matches) > 0 and
                    len(matches) == sample["run_count"]):
                    # then:
                    sample["status"] = "ROOT"
                elif (CONFIG.IGNORE_NOROOT_WORKFLOWS and
                      sample["wm_status"] in CONFIG.FINAL_WM_STATUSES):
                    sample["status"] = "NoROOT"
                self.request.release_access()

    def stop(self):
        self._stop = True


class SSHWorker(Worker):
    def __init__(self, command):
        super(SSHWorker, self).__init__()
        self.command = command
        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    def run(self):
        self.ssh_client.connect(CONFIG.REMOTE_HOST,
                                username=credentials["user"],
                                password=credentials["pass"])
        (stdin, stdout, stderr) = self.ssh_client.exec_command(self.command)
        print(stdout.readlines())
        print(stderr.readlines())
        self.ssh_client.close()

    def stop(self):
        self.ssh_client.close()


class Downloader(SSHWorker):
    """Documentation for Downloader

    """
    def __init__(self, request):
        super(Downloader, self).__init__(
            "cd " + CONFIG.REMOTE_WORK_DIR + ';' +
            "./download_ROOT.py " + str(request.id_))


class Reporter(SSHWorker):
    """Documentation for Reporter

    """
    def __init__(self, request):
        super(Reporter, self).__init__(
            "cd " + CONFIG.REMOTE_CMSSW_DIR + ';' +
            "eval `scramv1 runtime -sh`" +
            "cd " + CONFIG.REMOTE_WORK_DIR + ';' +
            "./compare.py " + str(request.id_))


class Cleaner(SSHWorker):
    """Documentation for Cleaner

    """
    def __init__(self, request):
        super(Cleaner, self).__init__(
            "cd " + CONFIG.REMOTE_WORK_DIR + ';' +
            "./clean.py " + str(request.id_))
