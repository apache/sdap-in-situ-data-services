import os

# os.environ['master_spark_url'] = 'spark://bigdata-1.oceanworks:7077'
os.environ['master_spark_url'] = 'spark://localhost:7077'
os.environ['spark_app_name'] = 'pyspark-flask'
os.environ['parquet_file_name'] = 'CDMS_insitu.parquet'

def flask_me():
    import logging

    LOGGER = logging.getLogger(__name__)
    LOGGER.setLevel(logging.INFO)
    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)
    LOGGER.addHandler(sh)

    from gevent.pywsgi import WSGIServer
    from parquet_flask import get_app
    # get_app().run(host='0.0.0.0', port=9788, threaded=True)
    http_server = WSGIServer(('', 9801), get_app())
    http_server.serve_forever()
    return


if __name__ == '__main__':
    flask_me()
