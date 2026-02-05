from flask import Flask, request, jsonify
from .db import create_tables
from .worker import submit_job, recover_and_resubmit_running_jobs
from .models import Batch, BatchStatus
from .db import SessionLocal
import uuid
import time

def create_app():
    app = Flask(__name__)

    @app.before_first_request
    def startup():
        create_tables()
        recover_and_resubmit_running_jobs()

    @app.route("/request", methods=["POST"])
    def request_package():
        data = request.get_json(force=True)
        # parse and only accept pinned versions
        # create jobs via submit_job(...) which persists and schedules
        # return job ids or block until complete by polling DB
        ...
    # other endpoints...
    return app
