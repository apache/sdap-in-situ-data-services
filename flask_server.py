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
        level=log_level,
        format=log_format,
        handlers=[file_handler, stream_handler]
    )

    LOGGER = logging.getLogger(__name__)
    LOGGER.setLevel(logging.INFO)

    from gevent.pywsgi import WSGIServer
    from parquet_flask import get_app
    # get_app().run(host='0.0.0.0', port=9788, threaded=True)
    http_server = WSGIServer(('', 9801), get_app())
    http_server.serve_forever()
    return


if __name__ == '__main__':
    flask_me()
