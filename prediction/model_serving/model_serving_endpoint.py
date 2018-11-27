from os import getenv
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
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
        self.session.default_fetch_size = 1

        self.prep_stmt = self.session.prepare(self.QUERY)

    def retrieve_prediction(self, client_id):
        bind_list = [client_id]
        return self.session.execute(self.prep_stmt, bind_list, timeout=self.CASS_REQ_TIMEOUT)._current_rows

    def retrieve_predictions(self, paging_state=''):
        query = "SELECT * FROM ga_chp_predictions"
        statement = SimpleStatement(query, fetch_size=100)
        predictions = {}

        if paging_state != '':
            if not re.match('^[a-zA-Z0-9_]+$', paging_state):
                predictions['status'] = 0
                predictions['error'] = 'Invalid page format.'
                return predictions

            previous_paging_state = bytes.fromhex(paging_state)

            try:
                results = self.session.execute(
                    statement, paging_state=previous_paging_state, timeout=self.CASS_REQ_TIMEOUT)
            except ProtocolException:
                predictions['status'] = 0
                predictions['error'] = 'Invalid pagination request.'
                return predictions

        else:
            results = self.session.execute(
                statement, timeout=self.CASS_REQ_TIMEOUT)

        predictions['next_paging_state'] = results.paging_state.hex(
        ) if results.has_more_pages == True else 0
        predictions['predictions'] = results._current_rows
        predictions['status'] = 1

        return predictions

    def get_predictions_number(self):
        query = "SELECT COUNT(*) FROM ga_chp_predictions"

        statement = SimpleStatement(query)

        return self.session.execute(statement, timeout=self.CASS_REQ_TIMEOUT)._current_rows[0].count

    def get_churned_number(self):
        query = "SELECT COUNT(*) FROM ga_chp_predictions WHERE prediction > 0.5 ALLOW FILTERING"

        statement = SimpleStatement(query)

        return self.session.execute(statement, timeout=self.CASS_REQ_TIMEOUT)._current_rows[0].count

    def get_model_statistics(self):

        query = "SELECT accuracy, loss, day_as_str FROM ga_chp_valid_models WHERE is_model_valid = True LIMIT 20 ALLOW FILTERING;"

        statement = SimpleStatement(query)

        data_rows = self.session.execute(
            statement, timeout=self.CASS_REQ_TIMEOUT)

        return data_rows._current_rows


"""
    API class for verifying credentials and handling JWTs.
"""


class API:
    def __init__(self):
        self.API_DOMAIN = getenv('API_DOMAIN')
        self.MORPHL_DASHBOARD_USERNAME = getenv('MORPHL_DASHBOARD_USERNAME')
        self.MORPHL_DASHBOARD_PASSWORD = getenv('MORPHL_DASHBOARD_PASSWORD')
        self.MORPHL_API_KEY = getenv('MORPHL_API_KEY')
        self.MORPHL_API_SECRET = getenv('MORPHL_API_SECRET')
        self.MORPHL_API_JWT_SECRET = getenv('MORPHL_API_JWT_SECRET')

        # Set JWT expiration date at 30 days
        self.JWT_EXP_DELTA_DAYS = 30

    def verify_login_credentials(self, username, password):
        return username == self.MORPHL_DASHBOARD_USERNAME and password == self.MORPHL_DASHBOARD_PASSWORD

    def verify_keys(self, api_key, api_secret):
        return api_key == self.MORPHL_API_KEY and api_secret == self.MORPHL_API_SECRET

    def generate_jwt(self):
        payload = {
            'iss': self.API_DOMAIN,
            'sub': self.MORPHL_API_KEY,
            'iat': datetime.utcnow(),
            'exp': datetime.utcnow() + timedelta(days=self.JWT_EXP_DELTA_DAYS),
        }

        return jwt.encode(payload, self.MORPHL_API_JWT_SECRET, 'HS256').decode('utf-8')

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


@app.route("/")
def main():
    return "MorphL Predictions API"


@app.route("/dashboard/login", methods=['POST'])
def authorize_login():

    if request.form.get('username') is None or request.form.get('password') is None:
        return jsonify(status=0, error='Missing username or password.')

    if not app.config['API'].verify_login_credentials(request.form['username'], request.form['password']):
        return jsonify(status=0, error='Invalid username or password.')

    return jsonify(status=1, token=app.config['API'].generate_jwt())


@app.route("/dashboard/verify-token", methods=['GET'])
def verify_token():

    if request.headers.get('Authorization') is None or not app.config['API'].verify_jwt(request.headers['Authorization']):
        return jsonify(status=0, error="Token invalid.")
    return jsonify(status=1)


@app.route('/getprediction/<client_id>')
def get_prediction(client_id):
    # Validate authorization header with JWT
    if request.headers.get('Authorization') is None or not app.config['API'].verify_jwt(request.headers['Authorization']):
        return jsonify(status=0, error='Unauthorized request')

    # Validate client id (alphanumeric with dots)
    if not re.match('^[a-zA-Z0-9.]+$', client_id):
        return jsonify(status=0, error='Invalid client id.')

    p = app.config['CASSANDRA'].retrieve_prediction(client_id)

    if len(p) == 0:
        return jsonify(status=0, error='No associated predictions found for that ID.')

    return jsonify(status=1, prediction={'client_id': client_id, 'prediction': p[0].prediction})


@app.route('/getpredictions', methods=['GET'])
def get_predictions():

    if request.headers.get('Authorization') is None or not app.config['API'].verify_jwt(request.headers['Authorization']):
        return jsonify(status=0, error='Unauthorized request.')

    if request.args.get('page') is None:
        return jsonify(app.config['CASSANDRA'].retrieve_predictions())

    predictions = app.config['CASSANDRA'].retrieve_predictions(
        paging_state=request.args.get('page'))

    return jsonify(predictions)


@app.route('/getpredictionstatistics', methods=['GET'])
def get_prediction_statistics():

    if request.headers.get('Authorization') is None or not app.config['API'].verify_jwt(request.headers['Authorization']):
        return jsonify(status=0, error='Unauthorized request.')

    predictions_number = app.config['CASSANDRA'].get_predictions_number()
    churned_number = app.config['CASSANDRA'].get_churned_number()
    modelStatistics = app.config['CASSANDRA'].get_model_statistics()
    not_churned_number = predictions_number - churned_number

    return jsonify(predictions_number=predictions_number, churned_number=churned_number, not_churned_number=not_churned_number, modelStatistics=modelStatistics, status=1)


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
