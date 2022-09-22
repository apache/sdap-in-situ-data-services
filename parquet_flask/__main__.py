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

import os
import sys

import findspark
findspark.init()

def flask_me():
    import logging
    log_format = '%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s'
    log_formatter = logging.Formatter(log_format)
    log_level = getattr(logging, os.getenv('log_level', 'INFO').upper(), None)
    if not isinstance(log_level, int):
        print(f'invalid log_level:{log_level}. setting to INFO')
        log_level = logging.INFO

    file_handler = logging.FileHandler('/tmp/parquet_flask.log', mode='a')
    file_handler.setLevel(log_level)
    file_handler.setFormatter(log_formatter)

    stream_handler = logging.StreamHandler(stream=sys.stdout)
    stream_handler.setLevel(level=log_level)
    stream_handler.setFormatter(log_formatter)

    logging.basicConfig(
        level=logging.ERROR,
        format=log_format,
        handlers=[stream_handler]
    )

    logger = logging.getLogger('parquet_flask')
    logger.addHandler(stream_handler)
    logger.setLevel(log_level)

    from gevent.pywsgi import WSGIServer
    from parquet_flask import get_app
    # get_app().run(host='0.0.0.0', port=9788, threaded=True)
    http_server = WSGIServer(('', 9801), get_app())
    http_server.serve_forever()
    return


if __name__ == '__main__':
    flask_me()
