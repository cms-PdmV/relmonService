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
from threading import Lock

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
                        logger.debug("RR: %s current status:%s" % (self.request.id_,
                                self.request.status))

                        if (self.request.status in ["initial", "waiting"]):
                            logger.info("RR %s is new or waiting" % self.request.id_)
                            self.request.status = "waiting"
                            waiting = True
                        if (waiting):
                            if (not self._update_statuses()):
                                logger.debug("Shit just got real!")
                        logger.debug("Before downloading=False")
                        downloading = False
                        logger.debug("downloading:%s" % (downloading))
                        if (self.request.status in ["waiting", "downloading"]):
                            self.request.status = "downloading"
                            logger.info("RR is/changed to  downloading")
                            downloading = True
                        if (downloading):
                            self._do_downloads()
                        comparing = False
                        if (self.request.status in ["downloaded", "Qued_to_compare"]):
                            self.request.status = "Qued_to_compare"
                            logger.info("RR is/changed to  Qued_to_compare")
                            comparing = True
                        if (comparing):
                            self._make_report()
                            self.request.status = "comparing"
                            shared.update(self.request.id_)
                logger.debug("Going to sleep for 30sec")
                time.sleep(30)
        except Exception as ex:
            logger.info("something went wrong. Crashed on Controler.run: %s" % (str(ex)))

    def _update_statuses(self):
        logger.info("Request id: %s" % (self.request.id_))
        relmon.StatusUpdater(self.request)
        logger.info("finished first update")
        if (self._stop):
            if (self._terminate):
                self._clean()
            return False

        shared.update(self.request.id_)
        if (self.request.status in CONFIG.FINAL_RELMON_STATUSES):
            logger.log("Request:%s already in final state" % (self.request.id_))
            return False

        if (self.request.is_download_ready()):
            logger.info("Request:%s RR download ready" % (self.request.id_))
            return True
        else:
            logger.info("Request:%s RR download not ready." % (self.request.id_))
            return False

        if (self.request.is_ROOT_100()):
            logger.info("Request:%s RR ROOT files at 100%" % (self.request.id_))
            return True

        ##TO-DO is one terminate enough in here?
        if (self._stop):
            if (self._terminate):
                self._clean()
            return False
        ##TO-DO really??
        shared.update(self.request.id_)

    def _do_downloads(self):

        if (relmon.lock_thread(self.request)):
            logger.info("afther acquiring thread")
            command = """\
                cd %s;
                ./download_ROOT.py %s
                > download_ROOT.out 2>&1
                """ % (CONFIG.REMOTE_WORK_DIR, str(self.request.id_))
            task = relmon.BeastBornToDownload(command, self.request)


            logger.info("Added:%s to queue DOWNLOAD" % (self.request.id_))
            logger.info("task: %s" %task.run)
            relmon.downloads_queue.add_task(task.run)
        else:
            logger.info("Error in qcquiring lock")

    def _make_report(self):
        logger.info("came to _make_report")
        if (relmon.lock_thread(self.request)):
            command ="""\
                cd %s;
                eval `scramv1 runtime -sh`
                cd %s;
                ./compare.py %s > compare.out 2>&1
                """ % (CONFIG.REMOTE_CMSSW_DIR, CONFIG.REMOTE_WORK_DIR, self.request.id_)
            task = relmon.BeastBornToCompare(command, self.request)
            relmon.reports_queue.add_task(task.run)
            logger.info("Added:%s to queue COMPARE" % (self.request.id_))
        else:
            logger.debug("Error in acquiring lock")

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
