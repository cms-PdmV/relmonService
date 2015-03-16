"""
Restful flask resources for relmon request service.
"""

from flask.ext.restful import Resource
import time
from flask import request
from common import utils, relmon_shared

# TODO: eliminate code duplication
# # try, except, return


class Sample(Resource):

    def get(self, request_id, category, sample_list, sample_name):
        try:
            relmon_request = [i for i in relmon_shared.data if
                              i["id"] == request_id][0]
            the_category = [i for i in relmon_request["categories"] if
                            i["name"] == category][0]
            the_list = the_category["lists"][sample_list]
            return [i for i in the_list if i["name"] == sample_name][0], 200
        except IndexError as err:
            print(err)
            return "Not Found", 404
        except Exception as ex:
            print(ex)
            return "Internal error", 500

    def put(self, request_id, category, sample_list, sample_name):
        try:
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
                    ignore=["NoDQMIO"])
                if (relmon_request["status"] == "downloading" and
                    frac_downloaded * 100 >= relmon_request["threshold"]):
                    # then:
                    relmon_request["status"] = "downloaded"
                relmon_shared.write_data()
                return "OK", 200
        except IndexError as err:
            print(err)
            return "Not Found", 404
        except Exception as ex:
            print(ex)
            return "Internal error", 500


class RequestStatus(Resource):

    def put(self, request_id):
        try:
            with relmon_shared.data_lock:
                relmon_request = [i for i in relmon_shared.data if
                                  i["id"] == request_id][0]
                # TODO: check new_status for validity
                new_status = request.json["value"]
                relmon_request["status"] = new_status
                relmon_shared.write_data()
                return "OK", 200
        except IndexError as err:
            print(err)
            return "Not Found", 404
        except Exception as ex:
            print(ex)
            return "Internal error", 500


class RequestLog(Resource):

    def put(self, request_id):
        try:
            with relmon_shared.data_lock:
                relmon_request = [i for i in relmon_shared.data if
                                  i["id"] == request_id][0]
                # TODO: check new_status for validity
                new_log_state = request.json["value"]
                relmon_request["log"] = new_log_state
                relmon_shared.write_data()
                return "OK", 200
        except IndexError as err:
            print(err)
            return "Not Found", 404
        except Exception as ex:
            print(ex)
            return "Internal error", 500


class Request(Resource):

    def get(self, request_id):
        try:
            return [i for i in relmon_shared.data if
                    i["id"] == request_id][0], 200
        except IndexError as err:
            print(err)
            return "Not Found", 404
        except Exception as ex:
            print(ex)
            return "Internal error", 500


class Requests(Resource):

    def get(self):
        return relmon_shared.data, 200

    def post(self):
        try:
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
                        tmp_sample = {"name": sample, "status": "initial"}
                        sample_list[sample_idx] = tmp_sample
                new_record["categories"].append(category)
            with relmon_shared.data_lock:
                relmon_shared.data.append(new_record)
                relmon_shared.write_data()
            return "OK", 200
        except (TypeError, ValueError) as err:
            print(err)
            return "Bad request", 400
        except IndexError as err:
            print(err)
            return "Not Found", 404
        except Exception as ex:
            print(ex)
            return "Internal error", 500


class Terminator(Resource):
    """Documentation for Terminator

    """
    def __init__(self):
        super(Terminator, self).__init__()

    def post(self, request_id):
        try:
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
        except IndexError as err:
            print(err)
            return "Not Found", 404
        except Exception as ex:
            print(ex)
            return "Internal error", 500
