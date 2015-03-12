"""
Daemon checks DQMIO datatier statuses and files existence in DQM ROOT
directory for each sample and starts downloader when enough ROOT files are
present.
"""
from fractions import Fraction
import time
from threading import Thread
from common import utils
from common.relmon_request_data import RR_data, RR_data_lock, write_RR_data
REMOTE_WORK_DIR = "/build/jdaugala/relmon"
DOWNLOADER_CMD = "cd " + REMOTE_WORK_DIR + "; ./download_DQM_ROOT.py "
REPORT_GENERATOR_CMD = ("cd /build/jdaugala/CMSSW_7_4_0_pre8\n" +
                        " eval `scramv1 runtime -sh`\n" +
                        "cd " + REMOTE_WORK_DIR +
                        "\n ./compare.py ")


class RelmonReportDaemon(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.setDaemon(True)
        self.downloaders = {}
        self.report_generators = {}

    def run(self):
        while True:
            with RR_data_lock:
                global RR_data
                for relmon_request in RR_data:
                    if (relmon_request["status"] in ["done", "failed"]):
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
                        print(frac_ROOT)
                        print(frac_threshold)
                        if (frac_ROOT >= frac_threshold):
                            print("start donloader")
                            self.start_downloader(rid)
                            relmon_request["status"] = "downloading"
                            write_RR_data()
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
                        if (not self.downloaders[rid].isAlive()):
                            frac_ROOT = utils.sample_fraction_by_status(
                                relmon_request, ["ROOT"], ["NoDQMIO"])
                            if (frac_ROOT > Fraction(0)):
                                self.start_downloader(rid)
                    # TODO: start report generation not in this daemon
                    if (relmon_request["status"] == "downloaded"):
                        self.start_report_generator(rid)
                        relmon_request["status"] = "comparing"
                        write_RR_data()
            print("sleep")
            time.sleep(13)
            print("wake")
        # end of while

    def update_ROOT_statuses(self, relmon_request):
        if (relmon_request["status"] not in ["initial", "DQMIO", "ROOT"]):
            return
        for category in relmon_request["categories"]:
            category_has_DQMIO_samples = False
            for sample_list in category["lists"].itervalues():
                for sample in sample_list:
                    if (sample["status"] in ["initial", "waiting"]):
                        (DQMIO_status, DQMIO_string) = (
                            utils.get_DQMIO_status(sample["name"]))
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
                    # FIXME: temporary decision
                    sample_list[0]["status"] = "failed"
                    relmon_request["status"] = "failed"
                    # clean up
                    return
                for sample_idx, sample in enumerate(sample_list):
                    if (sample["status"] != "DQMIO"):
                        continue
                    if any(sample["ROOT_file_name_part"] in
                           s for s in file_urls):
                        sample["status"] = "ROOT"
        write_RR_data()

    def start_downloader(self, request_id):
        if (request_id in self.downloaders and
            self.downloaders[request_id].isAlive()):
            # then
            return None
        downloader = utils.SSHThread(
            DOWNLOADER_CMD + str(request_id))
        self.downloaders[request_id] = downloader
        downloader.start()
        return downloader

    def start_report_generator(self, request_id):
        if (request_id in self.report_generators and
            self.report_generators[request_id].isAlive()):
            # then
            return None
        reporter = utils.SSHThread(
            REPORT_GENERATOR_CMD + str(request_id))
        self.report_generators[request_id] = reporter
        reporter.start()
        return reporter
