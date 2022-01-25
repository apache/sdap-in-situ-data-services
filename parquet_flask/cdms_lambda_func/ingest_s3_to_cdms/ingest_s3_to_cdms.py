import json
import logging
import os

import requests

from parquet_flask.aws.aws_ddb import AwsDdb, AwsDdbProps

from parquet_flask.cdms_lambda_func.lambda_func_env import LambdaFuncEnv

LOGGER = logging.getLogger(__name__)


class IngestS3ToCdms:
    def __init__(self):
        required_var = [LambdaFuncEnv.PARQUET_META_TBL_NAME,
                        LambdaFuncEnv.CDMS_BEARER_TOKEN,
                        LambdaFuncEnv.CDMS_DOMAIN]
        if not all([k in os.environ for k in required_var]):
            raise EnvironmentError(f'one or more missing env: {required_var}')

        ddb_props = AwsDdbProps()
        ddb_props.hash_key = 's3_url'
        ddb_props.tbl_name = os.environ.get(LambdaFuncEnv.PARQUET_META_TBL_NAME)
        self.__ddb = AwsDdb(ddb_props)
        self.__cdms_domain = os.environ.get(LambdaFuncEnv.CDMS_DOMAIN)

    def start(self, event):
        logging.basicConfig(level=int(os.environ.get(LambdaFuncEnv.LOG_LEVEL, logging.INFO)),
                            format="%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s")

        s3_url = 's3://<bucket>/<key>'  # TODO get from event
        put_body = {'s3_url': s3_url}
        ddb_record = self.__ddb.get_one_item(s3_url)
        header = {'Authorization': f'Bearer {os.environ.get(LambdaFuncEnv.CDMS_BEARER_TOKEN)}'}  # TODO this comes from Secret manager. not directly from env variable
        if ddb_record is None:
            put_url = f'{self.__cdms_domain}/1.0/ingest_json_s3'
        else:
            put_url = f'{self.__cdms_domain}/1.0/replace_json_s3'
            put_body['job_id'] = ddb_record['uuid']
        LOGGER.debug(f'putting {put_body} to {put_url}')
        result = requests.put(url=put_url,
                              data=json.dumps(put_body),
                              headers=header,
                              verify=False)
        LOGGER.info(f'ingest result: {result.status_code}')
        LOGGER.debug(f'ingest result details: {result.text}')
        return
