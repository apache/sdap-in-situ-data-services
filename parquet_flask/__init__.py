import logging

from flask import Flask
from .v1 import blueprint


def get_app():
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s")
    app = Flask(__name__)
    app.register_blueprint(blueprint)
    # api.init_app(app)
    # CORS(app)
    return app
