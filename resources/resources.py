"""Restful flask resources for relmon request service."""

import logging
try:  # Python 2.7+
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass
import os
import json

from flask.ext.restful import Resource
from flask import request, send_from_directory

import controller
from config import CONFIG
from common import shared, relmon, controllers

logger = logging.getLogger(__name__)
logger.addHandler(NullHandler())


def admin_only(func):
    """Decorate methods to make them accessable only for administrators"""
    def decorator(*args, **kwargs):
        try:
            (request.headers["Adfs-Login"] in CONFIG.ADMINISTRATORS)
            return func(*args, **kwargs)
        except:
            return func(*args, **kwargs)
            # return "Forbidden", 403
    return decorator


def authorize(func):
    """Decorate methods to do authorization"""
    def decorator(*args, **kwargs):
        try:
            (request.headers["Adfs-Login"] in CONFIG.AUTHORIZED_USERS)
            return func(*args, **kwargs)
        except:
            return "Forbidden", 403
    return decorator


def add_default_HTTP_returns(func):
    """Decorate methods to add default HTTP responses"""
    def decorator(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (TypeError, ValueError):
            logger.info("Response: Bad request", exc_info=True)
            return "Bad request", 400
        except IndexError:
            logger.info("Respnse: Not found", exc_info=True)
            return "Not Found", 404
        except Exception:
            logger.exception("Response: Internal error")
            return "Internal error", 500
    return decorator


class Sample(Resource):

    @authorize
    @add_default_HTTP_returns
    def get(self, request_id, category, sample_list, sample_name):
        relmon_request = shared.relmons[request_id]
        the_category = [i for i in relmon_request.categories if
                        i["name"] == category][0]
        the_list = the_category["lists"][sample_list]
        return [i for i in the_list if i["name"] == sample_name][0], 200

    @admin_only
    @add_default_HTTP_returns
    def put(self, request_id, category, sample_list, sample_name):
        logger.debug("request data: " + json.dumps(request.json))
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

    @admin_only
    @add_default_HTTP_returns
    def put(self, request_id):
        logger.debug("request data: " + json.dumps(request.json))
        relmon_request = shared.relmons[request_id]
        # TODO: check new_status for validity
        relmon_request.get_access()
        try:
            if (relmon_request.status not in CONFIG.FINAL_RELMON_STATUSES):
                relmon_request.status = request.json["value"]
                return "OK", 200
        finally:
            relmon_request.release_access()


class RequestLog(Resource):

    @admin_only
    @add_default_HTTP_returns
    def post(self, request_id):
        logger.info("POSTing logfile")
        relmon_request = shared.relmons[request_id]
        filename = str(request_id) + ".log"
        with open(os.path.join(CONFIG.LOGS_DIR, filename), 'w') as f:
            f.write(request.stream.read())
        relmon_request.get_access()
        try:
            relmon_request.log = True
        finally:
            relmon_request.release_access()
        return "OK", 200

    @authorize
    @add_default_HTTP_returns
    def get(self, request_id):
        shared.relmons[request_id]
        if (os.path.isfile(os.path.join(CONFIG.LOGS_DIR,
                                        str(request_id) + ".log"))):
            return send_from_directory(
                CONFIG.LOGS_DIR,
                (str(request_id) + ".log"),
                attachment_filename=(str(request_id) + ".log"))
        else:
            return "Log file does not exist", 404


class Request(Resource):

    @authorize
    @add_default_HTTP_returns
    def get(self, request_id):
        return shared.relmons[request_id].to_dict(), 200

    @authorize
    @add_default_HTTP_returns
    def post(self, request_id):
        data = request.json
        relmon_request = relmon.RelmonRequest(**(data));
        logger.debug("relmon_request object: %" %relmon_request);
        shared.update(relmon_request)
        controllers.controllers[relmon_request.id_] = (
            controller.Controller(relmon_request))
        controllers.controllers[relmon_request.id_].start()
        return "OK", 200


class Edit(Resource):

    @authorize
    @add_default_HTTP_returns
    def post(self, request_id):
        data = request.json
        data['id_'] = request_id
        relmon_request = relmon.RelmonRequest(**(data));
        logger.debug(relmon_request.to_dict());
        # controllers.controllers[request_id].stop()
        shared.updateEntireRequest(request_id, relmon_request)
        # controllers.controllers[relmon_request.id_] = (
        #     #controller.Controller(relmon_request))
        # controllers.controllers[relmon_request.id_].start()
        return "OK", 200    


class Requests(Resource):

    @authorize
    @add_default_HTTP_returns
    def get(self):
        requests = []
        with shared.lock:
            for relmon_request in shared.relmons.itervalues():
                requests.append(relmon_request.to_dict())
        return requests, 200

    @authorize
    @add_default_HTTP_returns
    def post(self):
        logger.debug("request data: " + json.dumps(request.json))
        relmon_request = relmon.RelmonRequest(**(request.json))
        shared.new(relmon_request)
        # controllers.controllers[relmon_request.id_] = (
        #     controller.Controller(relmon_request))
        # controllers.controllers[relmon_request.id_].start()
        return "OK", 200
     

class Terminator(Resource):
    """Documentation for Terminator
    """
    @authorize
    @add_default_HTTP_returns
    def post(self, request_id):
        relmon_request = shared.relmons[request_id]
        relmon_request.get_priority_access()
        if (relmon_request.status == "terminating"):
            logger.info("Response: Already terminating")
            return "Already terminating", 409
        relmon_request.status = "terminating"
        relmon_request.release_priority_access()
        controllers.controllers[request_id].terminate()
        return "OK", 200

    @admin_only
    @add_default_HTTP_returns
    def delete(self, request_id):
        shared.relmons[request_id]
        controllers.controllers.pop(request_id)
        shared.drop(request_id)
        if (os.path.exists(CONFIG.LOGS_DIR + str(request_id) + ".log")):
            os.remove(CONFIG.LOGS_DIR + str(request_id) + ".log")
        return "OK", 200


class Closer(Resource):
    """Documentation for Closer
    """
    @authorize
    @add_default_HTTP_returns
    def post(self, request_id):
        relmon_request = shared.relmons[request_id]
        if (relmon_request.status != "finished"):
            return "RelMon request not finished", 400
        controllers.controllers.pop(request_id)
        shared.drop(request_id)
        if (os.path.exists(CONFIG.LOGS_DIR + str(request_id) + ".log")):
            os.remove(CONFIG.LOGS_DIR + str(request_id) + ".log")
        return "OK", 200


class UserInfo(Resource):
    """Documentation for UserInfo
    """
    @authorize
    @add_default_HTTP_returns
    def get(self):
        return {"username": request.headers["Adfs-Login"],
               "name": request.headers["Adfs-Fullname"],
               "group": request.headers["Adfs-Group"],
               "email": request.headers["Adfs-Email"]}


class GUI(Resource):
    """Documentation for GUI
    """
    @authorize
    @add_default_HTTP_returns
    def get(self):
        return send_from_directory("static", "index.htm")
