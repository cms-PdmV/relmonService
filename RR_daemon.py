"""
Daemon checks DQMIO datatier statuses and files existence in DQM ROOT
directory for each sample and starts downloader when enough ROOT files are
present.
"""
from fractions import Fraction
import time
from threading import Thread
from common import utils, relmon_shared


class RelmonReportDaemon(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.setDaemon(True)

    # TODO: reduce indentation
    def run(self):
        while True:
            for relmon_request in relmon_shared.data:
                if (relmon_request["status"] in
                    ["finished", "failed", "terminating"]):
                    # then:
                    continue
                rid = relmon_request["id"]
                frac_threshold = (
                    Fraction(relmon_request["threshold"], 100))
                if (relmon_request["status"] in ["initial", "DQMIO"]):
                    # update "ROOT" statuses and start downloading if
                    # there are enough samples with "ROOT" status
                    self.update_ROOT_statuses(relmon_request)
                    frac_ROOT = utils.sample_fraction_by_status(
                        relmon_request, ["ROOT"], ["NoDQMIO"])
                relmon_shared.high_priority_q.join()
                with relmon_shared.data_lock:
                    if (relmon_request["status"] in ["initial", "DQMIO"]):
                        if (frac_ROOT >= frac_threshold):
                            utils.start_downloader(rid)
                            relmon_request["status"] = "downloading"
                            relmon_shared.write_data()
                if (relmon_request["status"] == "downloading"):
                    # update "ROOT" statuses if "waiting" or "initial"
                    # samples exist; also, start downloading again if
                    # previuos downloader finished but there exist
                    # "ROOT" samples
                    frac_waiting = utils.sample_fraction_by_status(
                        relmon_request,
                        status=["initial", "waiting"],
                        ignore=["NoDQMIO"])
                    if (frac_waiting > Fraction(0)):
                        self.update_ROOT_statuses(relmon_request)
                relmon_shared.high_priority_q.join()
                with relmon_shared.data_lock:
                    if (relmon_request["status"] == "downloading" and
                        not utils.is_downloader_alive(rid)):
                        # then:
                        frac_ROOT = utils.sample_fraction_by_status(
                            relmon_request, ["ROOT"], ["NoDQMIO"])
                        if (frac_ROOT > Fraction(0)):
                            utils.start_downloader(rid)
                # TODO: start report generation not in this daemon
                relmon_shared.high_priority_q.join()
                with relmon_shared.data_lock:
                    if (relmon_request["status"] == "downloaded"):
                        utils.start_reporter(rid)
                        relmon_request["status"] = "comparing"
                        relmon_shared.write_data()
            print("sleep")
            time.sleep(13)
            print("wake")
        # end of while

    # TODO: reduce indentation
    def update_ROOT_statuses(self, relmon_request):
        if (relmon_request["status"] not in ["initial", "DQMIO", "ROOT"]):
            return
        for category in relmon_request["categories"]:
            category_has_DQMIO_samples = False
            for sample_list in category["lists"].itervalues():
                for sample in sample_list:
                    if (sample["status"] not in ["initial", "waiting"]):
                        continue
                    (DQMIO_status, DQMIO_string) = (
                        utils.get_DQMIO_status(sample["name"]))
                    relmon_shared.high_priority_q.join()
                    with relmon_shared.data_lock:
                        if (relmon_request["status"] not in
                            ["initial", "DQMIO", "ROOT"]):
                            # then:
                            return
                        if (sample["status"] not in ["initial", "waiting"]):
                            continue
                        sample["status"] = DQMIO_status
                        if (DQMIO_status == "DQMIO"):
                            sample["ROOT_file_name_part"] = (
                                utils.get_ROOT_name_part(DQMIO_string))
                    if (sample["status"] == "DQMIO"):
                        category_has_DQMIO_samples = True
            if (not category_has_DQMIO_samples):
                continue
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
                if (not file_urls):
                    # FIXME: temporary solution
                    relmon_shared.high_priority_q.join()
                    with relmon_shared.data_lock:
                        if (relmon_request["status"] not in
                            ["initial", "DQMIO", "ROOT"]):
                            # then:
                            return
                        sample_list[0]["status"] = "failed"
                        relmon_request["status"] = "failed"
                        relmon_shared.write_data()
                    # clean up
                    return
                relmon_shared.high_priority_q.join()
                with relmon_shared.data_lock:
                    if (relmon_request["status"] not in
                        ["initial", "DQMIO", "ROOT"]):
                        # then:
                        return
                    for sample_idx, sample in enumerate(sample_list):
                        if (sample["status"] != "DQMIO"):
                            continue
                        if any(sample["ROOT_file_name_part"] in
                               s for s in file_urls):
                            sample["status"] = "ROOT"
