"""Relmon request service. Automating relmon reports production.""",

import sys
import logging
import logging.config

from flask import Flask
from flask.ext.restful import Api
from flask.ext.cors import CORS

from common import utils, controllers
from resources import resources
from config import CONFIG

logging.config.fileConfig("logging.ini", disable_existing_loggers=False)
logger = logging.getLogger(__name__)
stdout_handler = logging.StreamHandler(sys.stdout)
werkzeug_logger = logging.getLogger("werkzeug")
werkzeug_logger.addHandler(stdout_handler)

app = Flask(__name__, static_url_path="")
cors = CORS(app)
api = Api(app)


api.add_resource(resources.GUI, "/", endpoint="gui")
api.add_resource(resources.UserInfo, "/userinfo", endpoint="userinfo")
api.add_resource(resources.Requests,
                 "/requests",
                 endpoint="requests")
api.add_resource(resources.Request,
                 "/requests/<int:request_id>",
                 endpoint="request")
api.add_resource(resources.Edit,
                 "/request/edit/<int:request_id>",
                 endpoint="request/edit")
api.add_resource(resources.Sample,
                 "/requests/<int:request_id>/categories/<string:category>/" +
                 "lists/<string:sample_list>/samples/<string:sample_name>",
                 endpoint="sample")
api.add_resource(resources.RequestLog,
                 "/requests/<int:request_id>.log",
                 endpoint=".log")
api.add_resource(resources.RequestStatus,
                 "/requests/<int:request_id>/status",
                 endpoint="status")
api.add_resource(resources.Terminator, "/requests/<int:request_id>/terminator",
                 endpoint="terminator")
api.add_resource(resources.Closer, "/requests/<int:request_id>/close",
                 endpoint="close")
logger.info("Flask resources atached")

try:
    utils.init_validation_logs_dir()
    utils.prepare_remote()
    #utils.init_authentication_ticket_renewal()
    controllers.init_controllers()
    logger.info("Controllers initialized")
except:
    logger.exception("Uncaught exception")
    raise


if __name__ == '__main__':
    print("Service is about to start.")
    print("You may wish to check log files sometimes.")
    app.run(threaded=True, debug=True, use_reloader=False, host='0.0.0.0', port=8080,
            ssl_context=(CONFIG.HOST_CERT_PATH,
                         CONFIG.HOST_KEY_PATH))
