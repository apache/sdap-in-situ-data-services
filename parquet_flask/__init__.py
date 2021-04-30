from flask import Flask
from .v1 import blueprint


def get_app():
    app = Flask(__name__)
    app.register_blueprint(blueprint)
    # api.init_app(app)
    # CORS(app)
    return app
