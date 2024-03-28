# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import logging

from flask_restx import Resource, Namespace
from flask import request

from parquet_flask.io_logic.federated_collection_crud import FederatedCollectionCrud


api = Namespace('federated_collections', description="Querying insitu data in Elasticsearch")
LOGGER = logging.getLogger(__name__)


@api.route('', methods=["get", "put", "post", "delete"], strict_slashes=False)
@api.route('/', methods=["get", "put", "post", "delete"], strict_slashes=False)
class FederatedCollectionsCrudEndpoint(Resource):

    @api.expect()
    def put(self):
        provider = request.args.get('provider', '')
        project = request.args.get('project', '')
        if any([k is None for k in [provider, project]]):
            return {'message': 'invalid parameters. must provide both provider & project'}, 500
        try:
            FederatedCollectionCrud().insert(provider, project)
        except Exception as e:
            LOGGER.exception(f'error while inserting federated collection: {provider} - {project}')
            return {'message': f'error while inserting federated collection: {str(e)}'}, 500
        return {'message': 'inserted'}, 200

    @api.expect()
    def post(self):
        provider = request.args.get('provider', '')
        project = request.args.get('project', '')
        if any([k is None for k in [provider, project]]):
            return {'message': 'invalid parameters. must provide both provider & project'}, 500
        try:
            FederatedCollectionCrud().update(provider, project)
        except Exception as e:
            LOGGER.exception(f'error while updating federated collection: {provider} - {project}')
            return {'message': f'error while updating federated collection: {str(e)}'}, 500
        return {'message': 'updated'}, 200

    @api.expect()
    def delete(self):
        provider = request.args.get('provider', '')
        project = request.args.get('project', '')
        if any([k is None for k in [provider, project]]):
            return {'message': 'invalid parameters. must provide both provider & project'}, 500
        try:
            FederatedCollectionCrud().delete(provider, project)
        except Exception as e:
            LOGGER.exception(f'error while deleting federated collection: {provider} - {project}')
            return {'message': f'error while deleting federated collection: {str(e)}'}, 500
        return {'message': 'deleted'}, 200

    @api.expect()
    def get(self):
        try:
            result = FederatedCollectionCrud().get()
        except Exception as e:
            LOGGER.exception(f'error while getting federated collection')
            return {'message': f'error while getting federated collection: {str(e)}'}, 500
        return {'result': result}, 200
