from os import getenv

from flask import (render_template as rt,
                   Flask, request, redirect, url_for, session, jsonify)

from gevent.pywsgi import WSGIServer

app = Flask(__name__)

if __name__ == '__main__':
    if getenv('DEBUG'):
        app.config['DEBUG'] = True
        flask_port = 5858
        app.run(host='0.0.0.0', port=flask_port)
    else:
        app.config['DEBUG'] = False
        flask_port = 6868
        WSGIServer(('', flask_port), app).serve_forever()
