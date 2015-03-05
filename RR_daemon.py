"""
Daemon checks DQMIO datatier statuses and files existence in DQM ROOT
directory for each sample and starts downloader when enough ROOT files are
present.
"""

import time
from threading import Thread
import thread
from common import utils
from common.relmon_request_data import RR_data, RR_data_lock, write_RR_data


class RelmonReportDaemon(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.setDaemon(True)

    def run(self):
        while True:
            with RR_data_lock:
                global RR_data
                for relmon_request in RR_data:
                    if (relmon_request["status"] in ["done", "failed"]):
                        continue
                    if (relmon_request["status"] in ["initial", "DQMIO"]):
                        self.update_ROOT_statuses(relmon_request)
                        if (relmon_request["status"] == "ROOT"):
                            thread.start_new_thread(
                                utils.launch_downloads,
                                (relmon_request["id"],))
                    if (relmon_request["status"] == "downloaded"):
                        thread.start_new_thread(
                            utils.launch_validation_matrix,
                            (relmon_request["id"],))
                        relmon_request["status"] = "comparing"
                        write_RR_data()
            print("sleep")
            print(RR_data_lock)
            time.sleep(13)
        # end of while

    def update_ROOT_statuses(self, relmon_request):
        if (relmon_request["status"] not in ["initial", "DQMIO"]):
            return
        old_request_status = relmon_request["status"]
        relmon_request["status"] = "ROOT"
        for category in relmon_request["categories"]:
            category_has_DQMIO_samples = False
            category_has_waiting_samples = False
            for lname, sample_list in category["lists"].iteritems():
                for sample in sample_list:
                    if (sample["status"] in ["initial", "waiting"]):
                        (DQMIO_status, DQMIO_string) = (
                            utils.get_DQMIO_status(sample["name"]))
                        sample["status"] = DQMIO_status
                        if (DQMIO_status == "DQMIO"):
                            sample["ROOT_file_name_part"] = (
                                utils.get_ROOT_name_part(DQMIO_string))
                        elif (DQMIO_status == "waiting"):
                            category_has_waiting_samples = True
                    if (sample["status"] == "DQMIO"):
                        category_has_DQMIO_samples = True
            if (category_has_DQMIO_samples):
                category_has_DQMIO_samples = False
                for sample_list in category["lists"].itervalues():
                    # TODO: handle failures (util.get_ROOT_file_urls)
                    # # failed CMSSW parsing
                    # relmon_request["status"] = "failed"
                    # sample_list[0]["status"] = "failed"
                    # return
                    # # r.status_code != requests.codes.ok
                    # continue
                    file_urls = utils.get_ROOT_file_urls(
                        sample_list[0]["name"],
                        category["name"])
                    # TODO: handle failures
                    if (not file_urls):
                        print("sikna")
                        return
                    for sample_idx, sample in enumerate(sample_list):
                        if (sample["status"] != "DQMIO"):
                            continue
                        if any(sample["ROOT_file_name_part"] in
                               s for s in file_urls):
                            sample["status"] = "ROOT"
                        if (sample["status"] != "ROOT"):
                            category_has_DQMIO_samples = True
            if (category_has_DQMIO_samples or category_has_waiting_samples):
                relmon_request["status"] = old_request_status
        write_RR_data()
