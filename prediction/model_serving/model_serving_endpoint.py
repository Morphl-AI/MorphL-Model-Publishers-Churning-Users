from os import getenv
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

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


"""
    API class for verifying credentials and handling JWTs.
"""


class API:
    def __init__(self):
        self.API_DOMAIN = getenv('API_DOMAIN')
        self.MORPHL_API_KEY = getenv('MORPHL_API_KEY')
        self.MORPHL_API_SECRET = getenv('MORPHL_API_SECRET')
        self.MORPHL_API_JWT_SECRET = getenv('MORPHL_API_JWT_SECRET')
        # Set JWT expiration date at 30 days
        self.JWT_EXP_DELTA_DAYS = 30

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


@app.route('/authorize', methods=['POST'])
def authorize():

    if request.form.get('api_key') is None or request.form.get('api_secret') is None:
        return jsonify(error='Missing API key or secret')

    if app.config['API'].verify_keys(
            request.form['api_key'], request.form['api_secret']) == False:
        return jsonify(error='Invalid API key or secret')

    return jsonify(token=app.config['API'].generate_jwt())


@app.route('/getprediction/<client_id>')
def get_prediction(client_id):
    # Validate authorization header with JWT
    if request.headers.get('Authorization') is None or !app.config['API'].verify_jwt(request.headers['Authorization']):
        return jsonify(error='Unauthorized request')

    # Validate client id (alphanumeric with dots)
    if not re.match('^[a-zA-Z0-9.]+$', client_id):
        return jsonify(error='Invalid client id')

    p = app.config['CASSANDRA'].retrieve_prediction(client_id)
    p_dict = {'client_id': client_id}
    if len(p) == 0:
        p_dict['error'] = 'N/A'
    else:
        p_dict['churning'] = p[0].prediction

    return jsonify(prediction=p_dict)


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
