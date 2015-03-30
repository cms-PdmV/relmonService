import time
import threading
from common import relmon, shared
import config as CONFIG

# worker enums
WORKER_UPDATER = 1
WORKER_DOWNLOADER = 2
WORKER_REPORTER = 3
WORKER_CLEANER = 4


class Controller(threading.Thread):
    """Documentation for Controller

    """
    def __init__(self, request):
        super(Controller, self).__init__()
        self.request = request
        self.worker = None
        self._stop = False
        self._terminate = False

    def run(self):
        # print("controller", self.request.id_)
        # --- status updates
        waiting = False
        self.request.get_access()
        try:
            if (self.request.status in ["initial", "waiting"]):
                # print("initial waiting")
                self.request.status = "waiting"
                waiting = True
        finally:
            self.request.release_access()
        while(waiting):
            # print("while waiting")
            self._start_worker(WORKER_UPDATER)
            self.worker.join()
            if (self._stop):
                if (self._terminate):
                    self._clean()
                return
            shared.update(self.request.id_)
            if (self.request.status in CONFIG.FINAL_RELMON_STATUSES):
                return
            if (self.request.is_download_ready()):
                # print("download ready")
                break
            else:
                time.sleep(CONFIG.TIME_BETWEEN_STATUS_UPDATES)
        if (waiting and not self.request.is_ROOT_100()):
            # print("not root 100")
            time.sleep(CONFIG.TIME_AFTER_THRESHOLD_REACHED)
            self._start_worker(WORKER_UPDATER)
            self.worker.join()
            if (self._stop):
                if (self._terminate):
                    self._clean()
                return
            shared.update(self.request.id_)
        # --- end of status updates
        # --- download
        downloading = False
        self.request.get_access()
        try:
            if (self.request.status in ["waiting", "downloading"]):
                # print("change to download")
                self.request.status = "downloading"
                downloading = True
        finally:
            self.request.release_access()
        while(downloading):
            # print("while downloading")
            self._start_worker(WORKER_DOWNLOADER)
            self.worker.join()
            if (self._stop):
                if (self._terminate):
                    self._clean()
                return
            shared.update(self.request.id_)
            if (self.request.status in CONFIG.FINAL_RELMON_STATUSES):
                return
            if (self.request.has_ROOT()):
                # print("has root")
                time.sleep(CONFIG.TIME_BETWEEN_DOWNLOADS)
            else:
                break
        # --- end of download
        # --- make report
        comparing = False
        self.request.get_access()
        try:
            if (self.request.status in ["downloading", "comparing"]):
                # print("change to comparing")
                self.request.status = "comparing"
                comparing = True
        finally:
            self.request.release_access()
        if (comparing):
            # print("comparing")
            self._start_worker(WORKER_REPORTER)
            self.worker.join()
            shared.update(self.request.id_)
        if (self._stop):
            if (self._terminate):
                self._clean()
            return
        # --- end of make report

    def stop(self):
        self._stop = True
        if (self.worker):
            self.worker.stop()

    def terminate(self):
        # print(self.is_alive())
        self._terminate = True
        self.stop()
        if (not self.is_alive()):
            self._start_worker(WORKER_CLEANER)

    def _clean(self):
        self._start_worker(WORKER_CLEANER)
        self.worker.join()

    def _start_worker(self, worker_enum):
        print("start worker", worker_enum)
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
