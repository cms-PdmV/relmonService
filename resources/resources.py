"""
Restful flask resources for relmon request service.
"""

from flask.ext.restful import Resource
import time
from flask import request
from common import utils, relmon_shared


def add_default_HTTP_returns(func):
    def decorator(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (TypeError, ValueError) as err:
            print(err)
            return "Bad request", 400
        except IndexError as err:
            print(err)
            return "Not Found", 404
        except Exception as ex:
            print(ex)
            return "Internal error", 500
    return decorator


class Sample(Resource):

    @add_default_HTTP_returns
    def get(self, request_id, category, sample_list, sample_name):
        relmon_request = [i for i in relmon_shared.data if
                          i["id"] == request_id][0]
        the_category = [i for i in relmon_request["categories"] if
                        i["name"] == category][0]
        the_list = the_category["lists"][sample_list]
        return [i for i in the_list if i["name"] == sample_name][0], 200

    @add_default_HTTP_returns
    def put(self, request_id, category, sample_list, sample_name):
        with relmon_shared.data_lock:
            relmon_request = [i for i in relmon_shared.data if
                              i["id"] == request_id][0]
            the_category = [i for i in relmon_request["categories"] if
                            i["name"] == category][0]
            the_list = the_category["lists"][sample_list]
            for sidx, sample in enumerate(the_list):
                if (sample["name"] == sample_name):
                    the_list[sidx] = request.json
                    break
            frac_downloaded = utils.sample_fraction_by_status(
                relmon_request,
                status=["downloaded"],
                ignore=["NoDQMIO", "NoROOT"])
            if (relmon_request["status"] == "downloading" and
                frac_downloaded * 100 >= relmon_request["threshold"]):
                # then:
                utils.start_reporter(request_id)
                relmon_request["status"] = "comparing"
            relmon_shared.write_data()
            return "OK", 200


class RequestStatus(Resource):

    @add_default_HTTP_returns
    def put(self, request_id):
        with relmon_shared.data_lock:
            relmon_request = [i for i in relmon_shared.data if
                              i["id"] == request_id][0]
            # TODO: check new_status for validity
            if (relmon_request["status"] not in ["terminating", "finished"]):
                new_status = request.json["value"]
                relmon_request["status"] = new_status
                relmon_shared.write_data()
                return "OK", 200


class RequestLog(Resource):

    @add_default_HTTP_returns
    def put(self, request_id):
        with relmon_shared.data_lock:
            relmon_request = [i for i in relmon_shared.data if
                              i["id"] == request_id][0]
            # TODO: check new_status for validity
            new_log_state = request.json["value"]
            relmon_request["log"] = new_log_state
            relmon_shared.write_data()
            return "OK", 200


class Request(Resource):

    @add_default_HTTP_returns
    def get(self, request_id):
        return [i for i in relmon_shared.data if
                i["id"] == request_id][0], 200


class Requests(Resource):

    @add_default_HTTP_returns
    def get(self):
        return relmon_shared.data, 200

    @add_default_HTTP_returns
    def post(self):
        args = request.json
        new_record = {"id": int(time.time()),
                      "name": args["name"],
                      "status": "initial",
                      "threshold": int(args["threshold"]),
                      "log": False,
                      "categories": []}
        if (new_record["threshold"] < 0 or new_record["threshold"] > 100):
            raise ValueError
        for category in args["categories"]:
            for list_idx, sample_list in category["lists"].iteritems():
                for sample_idx, sample in enumerate(sample_list):
                    tmp_sample = {"name": sample,
                                  "status": "initial",
                                  "wm_status": ""}
                    sample_list[sample_idx] = tmp_sample
            new_record["categories"].append(category)
        with relmon_shared.data_lock:
            relmon_shared.data.append(new_record)
            relmon_shared.write_data()
        return "OK", 200


class Terminator(Resource):
    """Documentation for Terminator

    """
    def __init__(self):
        super(Terminator, self).__init__()

    @add_default_HTTP_returns
    def post(self, request_id):
        relmon_shared.high_priority_q.put(self)
        relmon_request = [i for i in relmon_shared.data if
                          i["id"] == request_id][0]
        if (utils.is_terminator_alive(request_id)):
            return "Already terminating", 409
        with relmon_shared.data_lock:
            relmon_request["status"] = "terminating"
        relmon_shared.high_priority_q.get()
        relmon_shared.high_priority_q.task_done()
        utils.start_terminator(request_id)
        return "OK", 200
