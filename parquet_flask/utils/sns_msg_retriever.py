#  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#  Copyright 2023, by the California Institute of Technology. ALL RIGHTS RESERVED.
#  United States Government Sponsorship acknowledged. Any commercial use must be
#  negotiated with the Office of Technology Transfer at the California Institute of
#  Technology.  This software is subject to U.S. export control laws and regulations
#  and has been classified as EAR99.  By accepting this software, the user agrees to
#  comply with all applicable U.S. export laws and regulations.  User has the
#  responsibility to obtain export licenses, or other export authority as may be
#  required before exporting such information to foreign countries or providing
#  access to foreign persons.
#  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

import json
import logging

from parquet_flask.utils.general_utils import GeneralUtils

LOGGER = logging.getLogger(__name__)


class LambdaEventMsgRetriever:
    SQS_MSG_SCHEMA = {
        'type': 'object',
        'properties': {
            'Records': {
                'type': 'array',
                'minItems': 1,
                'maxItems': 1,  # TODO only accept 1 item?
                'items': {
                    'type': 'object',
                    'properties': {
                        'body': {'type': 'string', 'minLength': 1}
                    },
                    'required': ['body']
                }
            }
        },
        'required': ['Records']
    }

    SNS_MSG_SCHEMA = {
        "type": "object",
        "properties": {
            "Type": {"type": "string"},
            "MessageId": {"type": "string"},
            "TopicArn": {"type": "string"},
            "Subject": {"type": "string"},
            "Timestamp": {"type": "string"},
            "SignatureVersion": {"type": "string"},
            "Signature": {"type": "string"},
            "SigningCertURL": {"type": "string"},
            "UnsubscribeURL": {"type": "string"},
            "Message": {"type": "string"},
        },
        "required": ["Message"]
    }

    S3_RECORD_SCHEMA = {
        'type': 'object',
        'properties': {'Records': {
            'type': 'array',
            'minItems': 1,
            'maxItems': 1,
            'items': {
                'type': 'object',
                'properties': {
                    'eventName': {'type': 'string'},
                    's3': {
                        'type': 'object',
                        'properties': {
                            'bucket': {
                                'type': 'object',
                                'properties': {'name': {'type': 'string', 'minLength': 1}},
                                'required': ['name']
                            },
                            'object': {
                                'type': 'object',
                                'properties': {'key': {'type': 'string', 'minLength': 1}},
                                'required': ['key']
                            }},
                        'required': ['bucket', 'object']
                    }
                },
                'required': ['eventName', 's3']
            }
        }},
        'required': ['Records']
    }

    def __init__(self):
        self.a = 1

    def from_sqs(self, sqs_msg):
        result, errors = GeneralUtils.is_json_valid(sqs_msg, self.SQS_MSG_SCHEMA)
        if result is False:
            raise ValueError(f'sqs_msg did not pass SQS_MSG_SCHEMA: {errors}')

        # TODO validate sqs
        sns_msgs = []
        for each_msg in sqs_msg['Records']:
            sns_msg = json.loads(each_msg['body'])
            result, errors = GeneralUtils.is_json_valid(sns_msg, self.SNS_MSG_SCHEMA)
            if result is False:
                LOGGER.error(f'sns_msg did not pass SNS_MSG_SCHEMA: {errors}. msg: {sns_msg}')
                continue
            sns_msgs.append(json.loads(sns_msg['Message']))
        return sns_msgs

    def get_s3_from_sns(self, sns_msg_body):
        result, errors = GeneralUtils.is_json_valid(sns_msg_body, self.S3_RECORD_SCHEMA)
        if result is False:
            raise ValueError(f'sqs_msg did not pass SQS_MSG_SCHEMA: {errors}')
        s3_summary = {
            'eventName': sns_msg_body['Records'][0]['eventName'],
            'bucket': sns_msg_body['Records'][0]['s3']['bucket']['name'],
            'key': sns_msg_body['Records'][0]['s3']['object']['key'],
        }
        return s3_summary
