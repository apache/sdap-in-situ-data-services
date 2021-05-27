import findspark
findspark.init()


def flask_me():
    import logging

    file_handler = logging.FileHandler('/tmp/parquet_flask.log', mode='a')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s'))

    logging.basicConfig(
        level=logging.DEBUG,  # TODO set it from container level
        format="%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s",
        handlers=[file_handler]
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
