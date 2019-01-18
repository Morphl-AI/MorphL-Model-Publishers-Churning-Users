from os import getenv
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement, dict_factory
from cassandra.protocol import ProtocolException

from operator import itemgetter

from flask import (render_template as rt,
                   Flask, request, redirect, url_for, session, jsonify)
from flask_cors import CORS

from gevent.pywsgi import WSGIServer

import jwt
import re
from datetime import datetime, timedelta

"""
    Database connector
"""


class Cassandra:
    def __init__(self):
        self.MORPHL_SERVER_IP_ADDRESS = getenv('MORPHL_SERVER_IP_ADDRESS')
        self.MORPHL_CASSANDRA_USERNAME = getenv('MORPHL_CASSANDRA_USERNAME')
        self.MORPHL_CASSANDRA_PASSWORD = getenv('MORPHL_CASSANDRA_PASSWORD')
        self.MORPHL_CASSANDRA_KEYSPACE = getenv('MORPHL_CASSANDRA_KEYSPACE')

        self.QUERY = 'SELECT * FROM ga_chp_predictions WHERE client_id = ? LIMIT 1'
        self.CASS_REQ_TIMEOUT = 3600.0

        self.auth_provider = PlainTextAuthProvider(
            username=self.MORPHL_CASSANDRA_USERNAME,
            password=self.MORPHL_CASSANDRA_PASSWORD)
        self.cluster = Cluster(
            [self.MORPHL_SERVER_IP_ADDRESS], auth_provider=self.auth_provider)

        self.session = self.cluster.connect(self.MORPHL_CASSANDRA_KEYSPACE)
        self.session.row_factory = dict_factory
        self.session.default_fetch_size = 100

        self.prepare_statements()

    def prepare_statements(self):
        """
            Prepare statements for database select queries
        """
        self.prep_stmts = {
            'predictions': {},
            'models': {}
        }

        template_for_single_row = 'SELECT * FROM ga_chp_predictions WHERE client_id = ? LIMIT 1'
        template_for_multiple_rows = 'SELECT * FROM ga_chp_predictions'
        template_for_count = 'SELECT COUNT(*) FROM ga_chp_predictions'
        template_for_count_churned = 'SELECT COUNT(*) FROM ga_chp_predictions WHERE prediction > 0.5 ALLOW FILTERING'
        template_for_models_rows = 'SELECT accuracy, loss, day_as_str FROM ga_chp_valid_models WHERE is_model_valid = True LIMIT 20 ALLOW FILTERING'

        self.prep_stmts['predictions']['single'] = self.session.prepare(
            template_for_single_row)
        self.prep_stmts['predictions']['multiple'] = self.session.prepare(
            template_for_multiple_rows)
        self.prep_stmts['predictions']['count'] = self.session.prepare(
            template_for_count)
        self.prep_stmts['predictions']['count_churned'] = self.session.prepare(
            template_for_count_churned)
        self.prep_stmts['models']['multiple'] = self.session.prepare(
            template_for_models_rows)

    def retrieve_prediction(self, client_id):
        bind_list = [client_id]
        return self.session.execute(self.prep_stmts['predictions']['single'], bind_list, timeout=self.CASS_REQ_TIMEOUT)._current_rows

    def retrieve_predictions(self, paging_state=''):
        if paging_state != '':
            try:
                previous_paging_state = bytes.fromhex(paging_state)
                results = self.session.execute(
                    self.prep_stmts['predictions']['multiple'], paging_state=previous_paging_state, timeout=self.CASS_REQ_TIMEOUT)
            except (ValueError, ProtocolException):
                return {'status': 0, 'error': 'Invalid pagination request.'}

        else:
            results = self.session.execute(
                self.prep_stmts['predictions']['multiple'], timeout=self.CASS_REQ_TIMEOUT)

        return {
            'status': 1,
            'predictions': results._current_rows,
            'next_paging_state': results.paging_state.hex(
            ) if results.has_more_pages == True else 0
        }

    def get_predictions_number(self):
        return self.session.execute(self.prep_stmts['predictions']['count'], timeout=self.CASS_REQ_TIMEOUT)._current_rows[0]['count']

    def get_churned_number(self):
        return self.session.execute(self.prep_stmts['predictions']['count_churned'], timeout=self.CASS_REQ_TIMEOUT)._current_rows[0]['count']

    def get_model_statistics(self):
        return self.session.execute(self.prep_stmts['models']['multiple'], timeout=self.CASS_REQ_TIMEOUT)._current_rows


"""
    API class for verifying credentials and handling JWTs.
"""


class API:
    def __init__(self):
        self.API_DOMAIN = getenv('API_DOMAIN')
        self.MORPHL_API_KEY = getenv('MORPHL_API_KEY')
        self.MORPHL_API_JWT_SECRET = getenv('MORPHL_API_JWT_SECRET')

    def verify_jwt(self, token):
        try:
            decoded = jwt.decode(token, self.MORPHL_API_JWT_SECRET)
        except Exception:
            return False

        return (decoded['iss'] == self.API_DOMAIN and
                decoded['sub'] == self.MORPHL_API_KEY)


app = Flask(__name__)
CORS(app)

# @todo Check request origin for all API requests


@app.route("/churning")
def main():
    return "MorphL Predictions API - Churning Users"


@app.route('/churning/getprediction/<client_id>')
def get_prediction(client_id):
    # Validate authorization header with JWT
    if request.headers.get('Authorization') is None or not app.config['API'].verify_jwt(request.headers['Authorization']):
        return jsonify(status=0, error='Unauthorized request.'), 401

    # Validate client id (alphanumeric with dots)
    if not re.match('^[a-zA-Z0-9.]+$', client_id):
        return jsonify(status=0, error='Invalid client id.')

    p = app.config['CASSANDRA'].retrieve_prediction(client_id)

    if len(p) == 0:
        return jsonify(status=0, error='No associated predictions found for that ID.')

    return jsonify(status=1, prediction={'client_id': client_id, 'prediction': p[0]['prediction']})


@app.route('/churning/getpredictions', methods=['GET'])
def get_predictions():

    if request.headers.get('Authorization') is None or not app.config['API'].verify_jwt(request.headers['Authorization']):
        return jsonify(status=0, error='Unauthorized request.'), 401

    if request.args.get('page') is None:
        return jsonify(app.config['CASSANDRA'].retrieve_predictions())

    if not re.match('^[a-zA-Z0-9_]+$', request.args.get('page')):
        return jsonify(status=0, error='Invalid page format.')

    predictions = app.config['CASSANDRA'].retrieve_predictions(
        paging_state=request.args.get('page'))

    return jsonify(predictions)


@app.route('/churning/getpredictionstatistics', methods=['GET'])
def get_prediction_statistics():

    if request.headers.get('Authorization') is None or not app.config['API'].verify_jwt(request.headers['Authorization']):
        return jsonify(status=0, error='Unauthorized request.'), 401

    predictions_number = app.config['CASSANDRA'].get_predictions_number()
    churned_number = app.config['CASSANDRA'].get_churned_number()
    model_statistics = app.config['CASSANDRA'].get_model_statistics()
    not_churned_number = predictions_number - churned_number

    return jsonify(
        status=1,
        predictions=predictions_number,
        churned=churned_number,
        not_churned=not_churned_number,
        model_statistics=model_statistics
    )


if __name__ == '__main__':
    app.config['CASSANDRA'] = Cassandra()
    app.config['API'] = API()
    if getenv('DEBUG'):
        app.config['DEBUG'] = True
        flask_port = 5858
        app.run(host='0.0.0.0', port=flask_port)
    else:
        app.config['DEBUG'] = False
        flask_port = 6868
        WSGIServer(('', flask_port), app).serve_forever()
