import logging

from flask_restx import Resource, Namespace, fields
from parquet_flask.utils.config import Config
from parquet_flask.utils.file_utils import FileUtils

api = Namespace('cdms_schema', description="Retrieve CDMS JSON schema")
LOGGER = logging.getLogger(__name__)


@api.route('', methods=["get", "post"], strict_slashes=False)
@api.route('/', methods=["get", "post"], strict_slashes=False)
class CdmsSchema(Resource):
    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, args, kwargs)

    @api.expect()
    def get(self):
        json_schema_path = Config().get_value('in_situ_schema')
        if not FileUtils.file_exist(json_schema_path):
            return {'message': f'file not found: {json_schema_path}'}, 404
        json_schema = FileUtils.read_json(json_schema_path)
        if json_schema is None:
            return {'message': 'file is invalid json'}, 500
        return json_schema, 200
