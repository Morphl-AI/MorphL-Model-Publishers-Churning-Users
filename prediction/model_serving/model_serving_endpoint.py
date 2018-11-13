from os import getenv
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
from cassandra.protocol import ProtocolException

from flask import (render_template as rt,
                   Flask, request, redirect, url_for, session, jsonify)

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
                predictions['error'] = 1
                return predictions

            previous_paging_state = bytes.fromhex(paging_state)

            try:
                results = self.session.execute(statement, paging_state=previous_paging_state)
            except ProtocolException:
                predictions['error'] = 1
                return predictions

        else:
            results = self.session.execute(statement)

        predictions['next_paging_state'] = results.paging_state.hex() if results.has_more_pages == True else 0
        predictions['values'] = results._current_rows
        predictions['error'] = 0

        return predictions


"""
    API class for verifying credentials and handling JWTs.
"""


class API:
    def __init__(self):
        self.API_DOMAIN = getenv('API_DOMAIN')
        self.DASHBOARD_USERNAME = getenv('DASHBOARD_USERNAME')
        self.DASHBOARD_PASSWORD = getenv('DASHBOARD_PASSWORD')
        self.MORPHL_API_KEY = getenv('MORPHL_API_KEY')
        self.MORPHL_API_SECRET = getenv('MORPHL_API_SECRET')
        self.MORPHL_API_JWT_SECRET = getenv('MORPHL_API_JWT_SECRET')
        # Set JWT expiration date at 1 days
        self.JWT_EXP_DELTA_DAYS = 1

    def verify_login_credentials(self, username, password):
        return username == self.DASHBOARD_USERNAME and password == self.DASHBOARD_PASSWORD

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

# @todo Check request origin for all API requests


@app.route("/")
def main():
    return "MorphL Predictions API"


@app.route("/dashboard/login", methods=['POST'])
def authorize_login():

    if request.form.get('username') is None or request.form.get('password') is None:
        return jsonify(error='Missing username or password')

    if app.config['API'].verify_login_credentials(request.form['username'], request.form['password']) == False:
        return jsonify(error='Invalid username or password')

    return jsonify(token=app.config['API'].generate_jwt())


@app.route("/dashboard/verify-token", methods=['GET'])
def verify_token():

    if request.headers.get('Authorization') is None or app.config['API'].verify_jwt(request.headers['Authorization']) == False:
        return jsonify(error="Token invalid")
    
    return jsonify(error=0)


@app.route('/getprediction/<client_id>')
def get_prediction(client_id):
    # Validate authorization header with JWT
    if request.headers.get('Authorization') is None or app.config['API'].verify_jwt(request.headers['Authorization']) == False:
        return jsonify(error='Unauthorized request')

    # Validate client id (alphanumeric with dots)
    if not re.match('^[a-zA-Z0-9.]+$', client_id):
        return jsonify(error='Invalid client id')

    p = app.config['CASSANDRA'].retrieve_prediction(client_id)
    p_dict = {'client_id': client_id}
    if len(p) == 0:
        p_dict['error'] = 'N/A'
    else:
        p_dict['result'] = p[0].prediction

    return jsonify(prediction=p_dict)


@app.route('/getpredictions', methods=['POST'])
def get_predictions():

    if request.headers.get('Authorization') is None or app.config['API'].verify_jwt(request.headers['Authorization']) == False:
        return jsonify(error='Unathorized request')

    if request.form.get('page') is None:
        return jsonify(app.config['CASSANDRA'].retrieve_predictions())

    predictions = app.config['CASSANDRA'].retrieve_predictions(paging_state=request.form.get('page'))

    if predictions['error'] == 1:
        return jsonify(error='Bad request')

    return jsonify(predictions)
 
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
