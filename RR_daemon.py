"""
Daemon checks DQMIO datatier statuses and files existence in DQM ROOT
directory for each sample and starts downloader when enough ROOT files are
present.
"""
from fractions import Fraction
import time
import threading
from common import utils, relmon_shared, iters
import itertools

SLEEP_TIME = 10

IGNORE_NOROOT_WORKFLOWS = True
FINAL_WM_STATUSES = ["rejected", "rejected-archived",
                     "aborted-completed", "aborted-archived", "announced",
                     "normal-archived"]


class RelmonReportDaemon(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.setDaemon(True)

    # TODO: reduce indentation
    def run(self):
        while True:
            # iterate only over existing requests (new requests might
            # appear while iterating)
            relmon_request_iter = itertools.islice(
                relmon_shared.data, 0, len(relmon_shared.data))
            for relmon_request in relmon_request_iter:
                if (relmon_request["status"] in ["finished",
                                                 "failed",
                                                 "terminating"]):
                    continue
                rid = relmon_request["id"]
                if (relmon_request["status"] in ["initial", "waiting"] and
                    self.request_is_download_ready(relmon_request)):
                    # then:
                    relmon_shared.high_priority_q.join()
                    with relmon_shared.data_lock:
                        if (relmon_request["status"] not in
                            ["initial", "waiting"]):
                            # then:
                            continue
                        utils.start_downloader(rid)
                        relmon_request["status"] = "downloading"
                        relmon_shared.write_data()
                if (relmon_request["status"] == "downloading" and
                    not utils.is_downloader_alive(rid)):
                    self.run_samples_status_updates(relmon_request)
                    relmon_shared.high_priority_q.join()
                    with relmon_shared.data_lock:
                        if (not [x for x in iters.samples(
                                relmon_request["categories"],
                                ["ROOT"])]):
                            relmon_request["status"] = "failed"
                            continue
                        if (relmon_request["status"] == "downloading" and
                            not utils.is_downloader_alive(rid)):
                            # then:
                            utils.start_downloader(rid)
                # TODO: start report generation not in this daemon
                relmon_shared.high_priority_q.join()
                with relmon_shared.data_lock:
                    if (relmon_request["status"] == "downloaded"):
                        utils.start_reporter(rid)
                        relmon_request["status"] = "comparing"
                        relmon_shared.write_data()
            print("sleep")
            time.sleep(SLEEP_TIME)
            print("wake")
        # end of while

    def request_is_download_ready(self, relmon_request):
        self.run_samples_status_updates(relmon_request)
        frac_threshold = (
            Fraction(relmon_request["threshold"], 100))
        frac_ROOT = utils.sample_fraction_by_status(
            relmon_request, ["ROOT"], ["NoDQMIO", "NoROOT"])
        return frac_ROOT >= frac_threshold

    def run_samples_status_updates(self, relmon_request):
        """Update sample statuses and return count of samples that changed
        their statuses to 'ROOT' by this update"""
        has_new_DQMIO = self.update_DQMIO_statuses(relmon_request)
        if (has_new_DQMIO):
            self.update_ROOT_names_parts(relmon_request)
            self.update_run_counts(relmon_request)
        self.update_wm_statuses(relmon_request)
        new_ROOT_count = self.update_ROOT_statuses(relmon_request)
        relmon_shared.write_data()
        return new_ROOT_count

    def update_DQMIO_statuses(self, relmon_request):
        relmon_request_has_DQMIO_samples = False
        sample_iter = iters.samples(
            relmon_request["categories"],
            ["initial", "waiting"])
        for sample in sample_iter:
            DQMIO_status, DQMIO_string = utils.get_DQMIO_status(sample["name"])
            relmon_shared.high_priority_q.join()
            with relmon_shared.data_lock:
                if (relmon_request["status"] not in
                    ["initial", "waiting", "downloading"]):
                    # then:
                    return
                # NOTE: assuming sample status has not changed in
                # other thread while checking DQMIO status
                sample["status"] = DQMIO_status
                sample["DQMIO_string"] = DQMIO_string
            if (sample["status"] == "DQMIO"):
                relmon_request_has_DQMIO_samples = True
        return relmon_request_has_DQMIO_samples

    def update_ROOT_names_parts(self, relmon_request):
        sample_iter = iters.samples(
            relmon_request["categories"],
            ["DQMIO"])
        for sample in sample_iter:
            relmon_shared.high_priority_q.join()
            with relmon_shared.data_lock:
                if (relmon_request["status"] not in
                    ["initial", "waiting", "downloading"]):
                    # then:
                    return
                sample["ROOT_file_name_part"] = (
                    utils.get_ROOT_name_part(sample["DQMIO_string"]))

    def update_run_counts(self, relmon_request):
        sample_iter = iters.samples(
            relmon_request["categories"],
            ["DQMIO"])
        for sample in sample_iter:
            run_count = utils.get_run_count(sample["DQMIO_string"])
            relmon_shared.high_priority_q.join()
            with relmon_shared.data_lock:
                if (relmon_request["status"] not in
                    ["initial", "waiting", "downloading"]):
                    # then:
                    return
                sample["run_count"] = run_count

    def update_wm_statuses(self, relmon_request):
        """Update sample statuses from Workload Manager"""
        sample_iter = iters.samples(
            relmon_request["categories"],
            ["initial", "waiting", "DQMIO"])
        for sample in sample_iter:
            wm_status = (
                utils.get_workload_manager_status(sample["name"]))
            if (sample["wm_status"] == wm_status):
                continue
            relmon_shared.high_priority_q.join()
            with relmon_shared.data_lock:
                if (relmon_request["status"] not in
                    ["initial", "waitnig", "downloading"]):
                    return
                sample["wm_status"] = wm_status

    # TODO: reduce indentation
    def update_ROOT_statuses(self, relmon_request):
        new_ROOT_sample_count = 0
        categories_listnames = itertools.product(
            relmon_request["categories"],
            ["target", "reference"])
        for (category, list_name) in categories_listnames:
            sample_list = category["lists"][list_name]
            if (not sample_list):
                continue
            file_urls = utils.get_ROOT_file_urls(
                sample_list[0]["name"],
                category["name"])
            if (not file_urls):
                # FIXME: temporary solution
                relmon_shared.high_priority_q.join()
                with relmon_shared.data_lock:
                    if (relmon_request["status"] not in
                        ["initial", "waiting", "downloading"]):
                        # then:
                        return
                    sample_list[0]["status"] = "failed"
                    relmon_request["status"] = "failed"
                    relmon_shared.write_data()
                # TODO: clean up
                return
            relmon_shared.high_priority_q.join()
            with relmon_shared.data_lock:
                if (relmon_request["status"] not in
                    ["initial", "waiting", "downloading"]):
                    # then:
                    return
                for sample in sample_list:
                    if (sample["status"] != "DQMIO"):
                        continue
                    matches = [u for u in file_urls
                               if (sample["ROOT_file_name_part"] in u)]
                    sample["root_count"] = len(matches)
                    if (len(matches) > 0 and
                        len(matches) == sample["run_count"]):
                        # then:
                        sample["status"] = "ROOT"
                        new_ROOT_sample_count += 1
                    elif (IGNORE_NOROOT_WORKFLOWS and
                          sample["wm_status"] in FINAL_WM_STATUSES):
                        sample["status"] = "NoROOT"
        return new_ROOT_sample_count
