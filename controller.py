"""RelMon report production controller. From workflow names to
completed report
"""

import logging
try:  # Python 2.7+
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass
import time
import threading
import json
import os
import random

from config import CONFIG
from common import relmon, shared

logger = logging.getLogger(__name__)
logger.addHandler(NullHandler())

# worker enums
WORKER_UPDATER = 1
WORKER_DOWNLOADER = 2
WORKER_REPORTER = 3
WORKER_CLEANER = 4


def uncaught_exception_to_log_decorator(func):
    """Decorate methods to log uncaught exceptions"""
    def decorator(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except:
            logger.exception("Uncaught exception in " + func.__name__)
            raise
    return decorator


class Controller(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)
        self.worker = None
        self.daemon = True
        self._stop = False
        self._terminate = False
        self.start()

    @uncaught_exception_to_log_decorator
    def run(self):
        try:
            while True:
                logger.info('Running main Controller')
                if (len(shared.relmons) > 0):
                    for key, value in shared.relmons.items():
                        self.request = shared.relmons[key]
                        waiting = False
                        if (self.request.status in ["initial", "waiting"]):
                            logger.info("RR %s is new or waiting" %self.request.id_)
                            self.request.status = "waiting"
                            waiting = True
                        if (waiting):
                            if (not self._update_statuses()):
                                return
                        # do downloads
                        downloading = False
                        if (self.request.status in ["waiting", "downloading"]):
                            self.request.status = "downloading"
                            logger.info("RR is/changed to  downloading")
                            downloading = True
                        if (downloading):
                            logger.info("invaiting _do_downloads!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                            command = """\
                                cd %s;
                                ./download_ROOT.py %s
                                > download_ROOT.out 2>&1
                                """ % (CONFIG.REMOTE_WORK_DIR, str(self.request.id_))
                            task = relmon.BeastBornToDownload(command, self.request)
                            self._do_downloads(task)
                    # make report

                        comparing = False
                        if (self.request.status in ["downloaded", "Qued_to_compare"]):
                            time.sleep(5)
                            self.request.status = "Qued_to_compare"
                            logger.info("RR is/changed to  Qued_to_compare")
                            comparing = True
                        if (comparing):
                            logger.info("invaiting _make_report!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                            task = relmon.BeastBornToCompare(command, self.request)
                            self._make_report(task)
                        # logger.info("Finished Controller for RR " + str(self.request.id_))
                time.sleep(60)
        except Exception as ex:
            logger.info("something went wrong. Crashed on Controler.run: %s" % (str(ex)))


# class Controller2(threading.Thread):
#     """Documentation for Controller

#     """
#     def __init__(self, request):
#         super(Controller, self).__init__()
#         self.request = request
#         self.worker = None
#         self._stop = False
#         self._terminate = False

#     @uncaught_exception_to_log_decorator
#     def run(self):
#         logger.info("Running Controller for RR " + str(self.request.id_))
#         waiting = False

#         try:
#             self.request.get_access()
#             if (self.request.status in ["initial", "waiting"]):
#                 logger.info("RR is new or waiting")
#                 self.request.status = "waiting"
#                 waiting = True
#         finally:
#             self.request.release_access()
#         if (waiting):
#             if (not self._update_statuses()):
#                 return
#         # do downloads
#         downloading = False
#         try:
#             self.request.get_access()
#             if (self.request.status in ["waiting", "downloading"]):
#                 self.request.status = "downloading"
#                 logger.info("RR is/changed to  downloading")
#                 downloading = True
#         finally:
#             self.request.release_access()
#         if (downloading):
#             if(not self._do_downloads()):
#                 return
#         # make report
#         comparing = False
        
#         try:
#             self.request.get_access()
#             if (self.request.status in ["downloading", "comparing"]):
#                 logger.info("RR is/changed to  comparing")
#                 self.request.status = "comparing"
#                 comparing = True
#         finally:
#             self.request.release_access()
#         if (comparing):
#             if (not self._make_report()):
#                 return

#         logger.info("Finished Controller for RR " + str(self.request.id_))

    def _update_statuses(self):
        while (True):
            # self._start_worker(WORKER_UPDATER)
            #?????????????????????????????????????????????????????????????????????//
            #?????????????????????????????????????????????????????????????????????//
            logger.info("Request id: %s" %self.request.id_)
            relmon.StatusUpdater(self.request)
            logger.info("finished first update")

            #?????????????????????????????????????????????????????????????????????//
            #?????????????????????????????????????????????????????????????????????//
            # self.worker.join()
            if (self._stop):
                if (self._terminate):
                    self._clean()
                return False
            shared.update(self.request.id_)
            #Commented, for a while. Because we don't want to make statuses Failed.
            if (self.request.status in CONFIG.FINAL_RELMON_STATUSES):
                 return False
            if (self.request.is_download_ready()):
                logger.info("RR download ready")
                return True
            else:
                logger.info("RR download not ready. Sleeping.")
                time.sleep(CONFIG.TIME_BETWEEN_STATUS_UPDATES)
                logger.info("Waking up")
        if (self.request.is_ROOT_100()):
            return True
        logger.info("RR not 100% root. Sleeping.")
        time.sleep(CONFIG.TIME_AFTER_THRESHOLD_REACHED)
        logger.info("Waking up")
        relmon.StatusUpdater(self.request)
        # self._start_worker(WORKER_UPDATER)
        # self.worker.join()
        if (self._stop):
            if (self._terminate):
                self._clean()
            return False
        shared.update(self.request.id_)

    def _do_downloads(self, a_simple_task):
        # while(True):

        logger.info("Added:%s to queue DOWNLOAD" % (self.request.id_))
        # logger.info("eil4: %s" %relmon.downloads_queue.tasks.queue)
        if not a_simple_task.run in relmon.downloads_queue.tasks.queue:

            relmon.downloads_queue.add_task(a_simple_task.run)
        else:
            logger.info("Task is already in Queueueuueueue")
        # if (self._stop):
        #     if (self._terminate):
        #         self._clean()
        #     return False
        # shared.update(self.request.id_)
        # if (self.request.status in CONFIG.FINAL_RELMON_STATUSES):  # ??
        #     return False                                           # ??
        # if (self.request.has_ROOT()):
        #     logger.info("RR has 'ROOT' workflows. Sleeping")
        #     time.sleep(CONFIG.TIME_BETWEEN_DOWNLOADS)
        #     logger.info("Waking up")
        # else:
        #     return True

        # while(True)
        #     self._start_worker(WORKER_DOWNLOADER)
        #     logger.info("Donwloads worker ret_code2: %s" %self.worker.ret_code)
        #     self.worker.join()
        #     logger.info("Donwloads worker ret_code3: %s" %self.worker.ret_code)

        #     if (self._stop):
        #         if (self._terminate):
        #             self._clean()
        #         return False
        #     logger.info("Donwloads worker ret_code4: %s" %self.worker.ret_code)
        #     shared.update(self.request.id_)
        #     if (self.worker.ret_code != 0):
        #         logger.error("Downloader on remote machine failed")
        #         self.request.get_access()
        #         try:
        #             self.request.status = "failed"
        #         finally:
        #             self.request.release_access()
        #             return False
        #     if (self.request.status in CONFIG.FINAL_RELMON_STATUSES):  # ??
        #         return False                                           # ??
        #     if (self.request.has_ROOT()):
        #         logger.info("RR has 'ROOT' workflows. Sleeping")
        #         time.sleep(CONFIG.TIME_BETWEEN_DOWNLOADS)
        #         logger.info("Waking up")
        #     else:
        #         return True

    def _make_report(self, a_simple_task):
        command ="""\
            cd %s;
            eval `scramv1 runtime -sh`
            cd %s;
            ./compare.py %s > compare.out 2>&1
            """ % (CONFIG.REMOTE_CMSSW_DIR, CONFIG.REMOTE_WORK_DIR, self.request.id_)

        if not a_simple_task.run in relmon.reports_queue.tasks.queue:
            relmon.reports_queue.add_task(a_simple_task.run)
            logger.info("Added:%s to queue COMPARE" % (self.request.id_))
        else:
            logger.info("Task is already in Queueueuueueue")

        # if (self._stop):
        #     if (self._terminate):
        #         self._clean()
        #     return False
        # shared.update(self.request.id_)
        # return True

    def stop(self):
        self._stop = True
        if (self.worker):
            self.worker.stop()
        logger.info("Stopping Controller for RR " + str(self.request.id_))

    def terminate(self):
        logger.info("Terminating Controller for RR " + str(self.request.id_))
        self._terminate = True
        self.stop()
        if (not self.is_alive()):
            self._start_worker(WORKER_CLEANER)
        # logger.info("Terminating Controller for RR " + str(self.request.id_))
        # self._terminate = True
        # self.stop()
        # if (not self.is_alive()):
        #     command ="""\
        #     cd %s;
        #     ./clean.py %s > clean.out 2>&1
        #     """ % (CONFIG.REMOTE_WORK_DIR, self.request.id_)

        # task = relmon.BeastOfLithuaniaWorker_Aivaras_YuriBoika_Sangokas_Alpha(command)
        # relmon.cleaners_queue.add_task(task.run())

    def _clean(self):
        logger.info("clean for RR " + str(self.request.id_))
        self._start_worker(WORKER_CLEANER)
        self.worker.join()
        if (self.worker.ret_code != 0):
            logger.error("Cleaner on remote machine failed")
            self.request.get_access()
            try:
                self.request.status = "failed"
            finally:
                self.request.release_access()
        # logger.info("clean for RR " + str(self.request.id_))
        # command ="""\
        #     cd %s;
        #     ./clean.py %s > clean.out 2>&1
        #     """ % (CONFIG.REMOTE_WORK_DIR, self.request.id_)

        # task = relmon.BeastOfLithuaniaWorker_Aivaras_YuriBoika_Sangokas_Alpha(command)
        # relmon.reports_queue.add_task(task.run())

    # def _start_worker(self, worker_enum):
    #     logger.info("Starting worker " + str(worker_enum))
    #     if (self.worker is not None and self.worker.is_alive()):
    #         raise RuntimeError("Other worker is still alive")
    #     elif (worker_enum == WORKER_UPDATER):
    #         self.worker = relmon.StatusUpdater(self.request)
    #     # elif (worker_enum == WORKER_DOWNLOADER):
    #     #     self.worker = relmon.Downloader(self.request)
    #     elif (worker_enum == WORKER_CLEANER):
    #         self.worker = relmon.Cleaner(self.request)
    #     self.worker.start()