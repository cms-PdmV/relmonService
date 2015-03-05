"""
Restful flask resources for relmon request service.
"""

from flask.ext.restful import Resource
from common.relmon_request_data import RR_data, RR_data_lock, write_RR_data
import time
from flask import request
from common import utils
import sys


class Sample(Resource):

    def get(self, request_id, category, sample_list, sample_name):
        global RR_data
        try:
            relmon_request = [i for i in RR_data if i["id"] == request_id][0]
            the_category = [i for i in relmon_request["categories"] if
                            i["name"] == category][0]
            the_list = the_category["lists"][sample_list]
            return [i for i in the_list if i["name"] == sample_name][0]
        except StandardError as err:
            print(err)
            return "Not Found", 404

    def put(self, request_id, category, sample_list, sample_name):
        with RR_data_lock:
            global RR_data
            try:
                relmon_request = (
                    [i for i in RR_data if i["id"] == request_id][0])
                the_category = [i for i in relmon_request["categories"] if
                                i["name"] == category][0]
                the_list = the_category["lists"][sample_list]
                for sidx, sample in enumerate(the_list):
                    if (sample["name"] == sample_name):
                        the_list[sidx] = request.json
                        break
                perc_downloaded = utils.sample_percent_by_status(
                    relmon_request,
                    status=["downloaded"],
                    ignore=["NoDQMIO"])
                if (
                        relmon_request["status"] == "ROOT" and
                        perc_downloaded >= float(relmon_request["threshold"])):
                    relmon_request["status"] = "downloaded"
                write_RR_data()
                return "OK", 200
            except StandardError as err:
                print(err)
                return "Not found", 404


class Request(Resource):

    def get(self, request_id):
        global RR_data
        response = [i for i in RR_data if i["id"] == request_id][0]
        if response:
            return response
        else:
            return [], 404


class Requests(Resource):

    def get(self):
        return RR_data

    def post(self):
        # parser = reqparse.RequestParser()
        # parser.add_argument("name", type=str)
        # parser.add_argument("threshold", type=float)
        # parser.add_argument("categories", type=list)
        args = request.json
        new_record = {"id": int(time.time()),
                      "name": args["name"],
                      "status": "initial",
                      "threshold": args["threshold"],
                      "categories": []}

        for category in args["categories"]:
            for list_idx, sample_list in category["lists"].iteritems():
                for sample_idx, sample in enumerate(sample_list):
                    tmp_sample = {"name": sample, "status": "initial"}
                    sample_list[sample_idx] = tmp_sample
            new_record["categories"].append(category)
        with RR_data_lock:
            global RR_data
            RR_data.append(new_record)
            write_RR_data()
        return args
