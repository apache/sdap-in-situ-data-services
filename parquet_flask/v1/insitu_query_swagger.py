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
from pathlib import Path

from flask import Blueprint, redirect, request, send_from_directory

apidocs_path = Path(__file__).parent.joinpath('insitu_query_swagger').resolve()
api = Blueprint('insitu_query_swagger', import_name=__name__, static_folder=apidocs_path, static_url_path='', url_prefix='/insitu_query_swagger')
LOGGER = logging.getLogger(__name__)

@api.route('', methods=["get", "post"])
def get_redirect():
    # Appends a / to the requested path
    return redirect(f'{request.path}/', 301)


@api.route('/', methods=["get", "post"], strict_slashes=False)
def get_index():
    return send_from_directory(apidocs_path, 'index.html')
