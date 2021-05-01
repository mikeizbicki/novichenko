# imports
import os
import time
import pspacy
import sqlalchemy
from sqlalchemy.sql import text
from flask import Flask, send_from_directory, render_template, g, request

# creates the flask app
app = Flask(__name__)
app.config.from_object('project.config.Config')

################################################################################
# routes
################################################################################

import project.routes.host
import project.routes.metahtml
import project.routes.ngrams
import project.routes.search


@app.route('/')
def index():
    return render_template(
        'index.html'
        )


@app.route("/static/<path:filename>")
def staticfiles(filename):
    return send_from_directory(app.config["STATIC_FOLDER"], filename)


################################################################################
# the code below creates a db connection and disconnects for each request;
# it replaces every occurrence of the string __EXECUTION_TIME__
# with the actual time to generate the webpage;
# this could result in some rendering bugs on some webpages
# see: https://stackoverflow.com/questions/12273889/calculate-execution-time-for-every-page-in-pythons-flask
################################################################################

engine = sqlalchemy.create_engine(app.config['DB_URI'], connect_args={
    'connect_timeout': 10,
    'application_name': 'novichenko/web',
    })


@app.before_request
def before_request():
    g.start = time.time()
    g.connection = engine.connect()


@app.after_request
def after_request(response):
    diff = time.time() - g.start
    diff_str = f'{"%0.3f"%diff} seconds'
    if ((response.response) and
        (200 <= response.status_code < 300) and
        (response.content_type.startswith('text/html'))):
        response.set_data(response.get_data().replace(
            b'__EXECUTION_TIME__', bytes(diff_str, 'utf-8')))
    return response


@app.teardown_request
def teardown_request(exception):
    if hasattr(g, 'connection'):
        g.connection.close()
