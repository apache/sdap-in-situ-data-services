from flask import Blueprint
from flask_restx import Api
from .ingest import api as ingest_parquet

_version = "1.0"

blueprint = Blueprint('parquet_flask', __name__, url_prefix='/{}'.format(_version))


api = Api(blueprint,
          title='Parquet ingestion & query',
          version=_version,
          description='API to support the Parquet ingestion & query data',
          doc='/doc/'
          )

# Register namespaces
api.add_namespace(ingest_parquet)
