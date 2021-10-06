from pathlib import Path

from flask_restx import Resource, Namespace
from flask import Blueprint, redirect, request, send_from_directory

apidocs_path = Path(__file__).parent.joinpath('apidocs').resolve()

api = Blueprint('apidocs', __name__, apidocs_path, '', url_prefix='/apidocs')

@api.get('')
def get_redirect():
    # Appends a / to the requested path
    return redirect(f'{request.path}/', 301)

@api.get('/')
def get_index():
    return send_from_directory(apidocs_path, 'index.html')
