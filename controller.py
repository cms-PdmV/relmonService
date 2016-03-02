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
    """Documentation for Controller

    """
    def __init__(self, request):
        super(Controller, self).__init__()
        self.request = request
        self.worker = None
        self._stop = False
        self._terminate = False

    @uncaught_exception_to_log_decorator
    def run(self):
        logger.info("Running Controller for RR " + str(self.request.id_))
        # upsdate statuses
        waiting = False
        self.request.get_access()
        try:
            if (self.request.status in ["initial", "waiting"]):
                logger.info("RR is new or waiting")
                self.request.status = "waiting"
                waiting = True
        finally:
            self.request.release_access()
        if (waiting):
            if (not self._update_statuses()):
                return
        # do downloads
        downloading = False
        self.request.get_access()
        try:
            if (self.request.status in ["waiting", "downloading"]):
                self.request.status = "downloading"
                logger.info("RR is/changed to  downloading")
                downloading = True
        finally:
            self.request.release_access()
        if (downloading):
            if(not self._do_downloads()):
                return
        # make report
        comparing = False
        self.request.get_access()
        try:
            if (self.request.status in ["downloading", "comparing"]):
                logger.info("RR is/changed to  comparing")
                self.request.status = "comparing"
                comparing = True
        finally:
            self.request.release_access()
        if (comparing):
            if (not self._make_report()):
                return

        logger.info("Finished Controller for RR " + str(self.request.id_))

    def _update_statuses(self):
        while(True):
            self._start_worker(WORKER_UPDATER)
            self.worker.join()
            if (self._stop):
                if (self._terminate):
                    self._clean()
                return False
            shared.update(self.request.id_)
            #Commented, for a while. Because we don't want to make statuses Failed.
            # if (self.request.status in CONFIG.FINAL_RELMON_STATUSES):
            #     return False
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
        self._start_worker(WORKER_UPDATER)
        self.worker.join()
        if (self._stop):
            if (self._terminate):
                self._clean()
            return False
        shared.update(self.request.id_)

    def _do_downloads(self):
        while(True):
            self._start_worker(WORKER_DOWNLOADER)
            self.worker.join()
            if (self._stop):
                if (self._terminate):
                    self._clean()
                return False
            shared.update(self.request.id_)
            if (self.worker.ret_code != 0):
                logger.error("Downloader on remote machine failed")
                self.request.get_access()
                try:
                    self.request.status = "failed"
                finally:
                    self.request.release_access()
                    return False
            if (self.request.status in CONFIG.FINAL_RELMON_STATUSES):  # ??
                return False                                           # ??
            if (self.request.has_ROOT()):
                logger.info("RR has 'ROOT' workflows. Sleeping")
                time.sleep(CONFIG.TIME_BETWEEN_DOWNLOADS)
                logger.info("Waking up")
            else:
                return True

    def _make_report(self):
        self._start_worker(WORKER_REPORTER)
        self.worker.join()
        if (self._stop):
            if (self._terminate):
                self._clean()
            return False
        shared.update(self.request.id_)
        if (self.worker.ret_code != 0):
            logger.error("Report generating on remote machine failed")
            self.request.get_access()
            try:
                self.request.status = "failed"
            finally:
                self.request.release_access()
                return False
        return True

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
                self.request.status = "failed controller 192"
            finally:
                self.request.release_access()

    def _start_worker(self, worker_enum):
        logger.info("Starting worker " + str(worker_enum))
        if (self.worker is not None and self.worker.is_alive()):
            raise RuntimeError("Other worker is still alive")
        elif (worker_enum == WORKER_UPDATER):
            self.worker = relmon.StatusUpdater(self.request)
        elif (worker_enum == WORKER_DOWNLOADER):
            self.worker = relmon.Downloader(self.request)
        elif (worker_enum == WORKER_REPORTER):
            self.worker = relmon.Reporter(self.request)
        elif (worker_enum == WORKER_CLEANER):
            self.worker = relmon.Cleaner(self.request)
        self.worker.start()
