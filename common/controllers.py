import controller
from common import shared

controllers = {}


def init_controllers():
    for relmon_request in shared.relmons.itervalues():
        controllers[relmon_request.id_] = controller.Controller(relmon_request)
        controllers[relmon_request.id_].start()
