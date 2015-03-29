"""
Restful flask resources for relmon request service.
"""
import os
import traceback
import controller
from flask.ext.restful import Resource
from flask import request
from common import shared, relmon

controllers = {}
for relmon_request in shared.relmons.itervalues():
    controllers[relmon_request.id_] = controller.Controller(relmon_request)
    controllers[relmon_request.id_].start()


def add_default_HTTP_returns(func):
    """Decorate methods to add default HTTP responses"""
    def decorator(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (TypeError, ValueError):
            traceback.print_exc()
            return "Bad request", 400
        except IndexError:
            traceback.print_exc()
            return "Not Found", 404
        except Exception:
            traceback.print_exc()
            return "Internal error", 500
    return decorator


class Sample(Resource):

    @add_default_HTTP_returns
    def get(self, request_id, category, sample_list, sample_name):
        relmon_request = shared.relmons[request_id]
        the_category = [i for i in relmon_request.categories if
                        i["name"] == category][0]
        the_list = the_category["lists"][sample_list]
        return [i for i in the_list if i["name"] == sample_name][0], 200

    @add_default_HTTP_returns
    def put(self, request_id, category, sample_list, sample_name):
        relmon_request = shared.relmons[request_id]
        the_category = [i for i in relmon_request.categories if
                        i["name"] == category][0]
        the_list = the_category["lists"][sample_list]
        for sidx, sample in enumerate(the_list):
            if (sample["name"] == sample_name):
                relmon_request.get_access()
                try:
                    the_list[sidx] = request.json
                finally:
                    relmon_request.release_access()
                break
        return "OK", 200


# TODO: think of other ways to change status internally
class RequestStatus(Resource):

    @add_default_HTTP_returns
    def put(self, request_id):
        relmon_request = shared.relmons[request_id]
        # TODO: check new_status for validity
        relmon_request.get_access()
        try:
            if (relmon_request.status not in relmon.FINAL_RELMON_STATUSES):
                relmon_request.status = request.json["value"]
                return "OK", 200
        finally:
            relmon_request.release_access()


class RequestLog(Resource):

    @add_default_HTTP_returns
    def put(self, request_id):
        relmon_request = shared.relmons[request_id]
        relmon_request.get_access()
        try:
            relmon_request.log = request.json["value"]
        finally:
            relmon_request.release_access()
        return "OK", 200


class Request(Resource):

    @add_default_HTTP_returns
    def get(self, request_id):
        return shared.relmons[request_id].to_dict(), 200


class Requests(Resource):

    @add_default_HTTP_returns
    def get(self):
        requests = []
        with shared.lock:
            for relmon_request in shared.relmons.itervalues():
                requests.append(relmon_request.to_dict())
        return requests, 200

    @add_default_HTTP_returns
    def post(self):
        relmon_request = relmon.RelmonRequest(**(request.json))
        shared.new(relmon_request)
        global controllers
        controllers[relmon_request.id_] = controller.Controller(relmon_request)
        controllers[relmon_request.id_].start()
        return "OK", 200


class Terminator(Resource):
    """Documentation for Terminator

    """
    @add_default_HTTP_returns
    def post(self, request_id):
        relmon_request = shared.relmons[request_id]
        relmon_request.get_priority_access()
        if (relmon_request.status == "terminating"):
            return "Already terminating", 409
        relmon_request.status = "terminating"
        relmon_request.release_priority_access()
        controllers[request_id].terminate()
        return "OK", 200

    def delete(self, request_id):
        shared.relmons[request_id]
        controllers.pop(request_id)
        shared.drop(request_id)
        if (os.path.exists("static/validation_logs/" +
                           str(request_id) + ".log")):
            os.remove("static/validation_logs/" +
                      str(request_id) + ".log")
        return "OK", 200
