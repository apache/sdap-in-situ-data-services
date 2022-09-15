from unittest import TestCase

from parquet_flask.cdms_lambda_func.s3_records.s3_2_sqs import S3ToSqs
from parquet_flask.utils.general_utils import GeneralUtils


class TestS32Sqs(TestCase):
    def test_01(self):
        event = {"Records": [{"messageId": "6210f778-d081-4ae9-a861-8534d612dfae", "receiptHandle": "AQEBk55DchogyQzpVsH1A4YEj4K/PcVuIG9Em/a6/4AHIA4G5vLPiHVElNiuMfYc1ussk2U//JwZbD788Fv8u6W22L3AJ1U8EIcGJ57aibpmd6tSCWLS5q5FA4u2X2Jq5z+lCX5NZXzNDYMqMJaCGtBkcYi4a9LDXtD+U7HWX0V8OPhFFF2a1qUu+E05c16f5OmE7wRJ3SFrRmtJOhp2DigKKsw6VJtZklTm6uILMOL1ETOTlbA02dhF16fjcXlAACirDp0Yo9pi91FrpEljOYkqAO9AX4WMbEjAPZrnaATfYmRqCTOlnrIK8xvgEPgIu/OOub7KBYh6AQn7U8QBNoASkXkn31dqyM2I+KosKy2VeJO9cjPTahhXtkW7zUFA6863Czt2oHqL6Rvwsjr+7TikfQ==", "body": "{\"Records\": [{\"eventVersion\": \"2.1\", \"eventSource\": \"aws:s3\", \"awsRegion\": \"us-gov-west-1\", \"eventTime\": \"2022-02-07T17:31:04.498Z\", \"eventName\": \"ObjectCreated:Put\", \"userIdentity\": {\"principalId\": \"AWS:AROAWM7XM4I6Z3NL2ST2J:wphyo\"}, \"requestParameters\": {\"sourceIPAddress\": \"128.149.246.219\"}, \"responseElements\": {\"x-amz-request-id\": \"FM1CHA780PBP0YEY\", \"x-amz-id-2\": \"Vp12Q/ok1+Y/0WonTCoUjCCREZhJU3CO82uDbve6m6FqJsFGTMBcLdunqeMLmQ11ZECV6z2WFsak6EbjdIZTi/jL+crmwops\"}, \"s3\": {\"s3SchemaVersion\": \"1.0\", \"configurationId\": \"all-obj-create\", \"bucket\": {\"name\": \"cdms-dev-ncar-in-situ-stage\", \"ownerIdentity\": {\"principalId\": \"440216117821\"}, \"arn\": \"arn:aws-us-gov:s3:::cdms-dev-ncar-in-situ-stage\"}, \"object\": {\"key\": \"cdms_icoads_2015-07-03.json.gz\", \"size\": 841141, \"eTag\": \"1477b70ad2cd03be3d72a49dc58fb52a\", \"sequencer\": \"0062015756ACFAA1FD\"}}}]}", "attributes": {"ApproximateReceiveCount": "6", "SentTimestamp": "1644255065441", "SenderId": "AIDALVP5ID7KAVBU2CQ3O", "ApproximateFirstReceiveTimestamp": "1644255065441"}, "messageAttributes": {}, "md5OfBody": "00cb0a5ed122862537ab6115dae36f69", "eventSource": "aws:sqs", "eventSourceARN": "arn:aws-us-gov:sqs:us-gov-west-1:440216117821:send_records_to_es", "awsRegion": "us-gov-west-1"}]}
        s3_url = S3ToSqs(event).get_s3_url(0)

        return

    def test_02(self):
        s3_event = {'Records': [
            {
                'messageId': '6210f778-d081-4ae9-a861-8534d612dfae',
                                 'receiptHandle': 'AQEBk55DchogyQzpVsH1A4YEj4K/PcVuIG9Em/a6/4AHIA4G5vLPiHVElNiuMfYc1ussk2U//JwZbD788Fv8u6W22L3AJ1U8EIcGJ57aibpmd6tSCWLS5q5FA4u2X2Jq5z+lCX5NZXzNDYMqMJaCGtBkcYi4a9LDXtD+U7HWX0V8OPhFFF2a1qUu+E05c16f5OmE7wRJ3SFrRmtJOhp2DigKKsw6VJtZklTm6uILMOL1ETOTlbA02dhF16fjcXlAACirDp0Yo9pi91FrpEljOYkqAO9AX4WMbEjAPZrnaATfYmRqCTOlnrIK8xvgEPgIu/OOub7KBYh6AQn7U8QBNoASkXkn31dqyM2I+KosKy2VeJO9cjPTahhXtkW7zUFA6863Czt2oHqL6Rvwsjr+7TikfQ==',
                                 'body': '{"Records": [{"eventVersion": "2.1", "eventSource": "aws:s3", "awsRegion": "us-gov-west-1", "eventTime": "2022-02-07T17:31:04.498Z", "eventName": "ObjectCreated:Put", "userIdentity": {"principalId": "AWS:AROAWM7XM4I6Z3NL2ST2J:wphyo"}, "requestParameters": {"sourceIPAddress": "128.149.246.219"}, "responseElements": {"x-amz-request-id": "FM1CHA780PBP0YEY", "x-amz-id-2": "Vp12Q/ok1+Y/0WonTCoUjCCREZhJU3CO82uDbve6m6FqJsFGTMBcLdunqeMLmQ11ZECV6z2WFsak6EbjdIZTi/jL+crmwops"}, "s3": {"s3SchemaVersion": "1.0", "configurationId": "all-obj-create", "bucket": {"name": "cdms-dev-ncar-in-situ-stage", "ownerIdentity": {"principalId": "440216117821"}, "arn": "arn:aws-us-gov:s3:::cdms-dev-ncar-in-situ-stage"}, "object": {"key": "cdms_icoads_2015-07-03.json.gz", "size": 841141, "eTag": "1477b70ad2cd03be3d72a49dc58fb52a", "sequencer": "0062015756ACFAA1FD"}}}]}',
                                 'attributes': {'ApproximateReceiveCount': '6', 'SentTimestamp': '1644255065441',
                                                'SenderId': 'AIDALVP5ID7KAVBU2CQ3O',
                                                'ApproximateFirstReceiveTimestamp': '1644255065441'},
                                 'messageAttributes': {}, 'md5OfBody': '00cb0a5ed122862537ab6115dae36f69',
                                 'eventSource': 'aws:sqs',
                                 'eventSourceARN': 'arn:aws-us-gov:sqs:us-gov-west-1:440216117821:send_records_to_es',
                                 'awsRegion': 'us-gov-west-1'
            }
                                ]
                    }
        is_valid, validation_err = GeneralUtils.is_json_valid(s3_event, S3ToSqs.S3_RECORD_SCHEMA)
        self.assertTrue(is_valid, validation_err)
        return
