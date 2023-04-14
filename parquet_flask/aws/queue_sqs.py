import json

from parquet_flask.aws.aws_cred import AwsCred
from parquet_flask.aws.queue_abstract import QueueAbstract
from parquet_flask.aws.queue_service_props import QueueServiceProps


class QueueSQS(QueueAbstract):
    def __init__(self, props: QueueServiceProps):
        super().__init__(props)
        client_params = {
            'service_name': 'sqs',
        }
        self.__sqs_client = AwsCred().get_client(**client_params)
        self.__sqs_url = self._props.queue_url

    def send_msg(self, sqs_msg: dict):
        self.__sqs_client.send_message(QueueUrl=self.__sqs_url, MessageBody=json.dumps(sqs_msg), DelaySeconds=0)
        return self