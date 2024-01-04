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
from parquet_flask.aws.aws_cred import AwsCred
from parquet_flask.aws.pub_sub_abstract import PubSubAbstract


class PubSubSns(PubSubAbstract):
    def __init__(self):
        super().__init__()
        client_params= {
            'service_name': 'sns',
        }
        self.__sns_client = AwsCred().get_client(**client_params)
        self.__topic_arn = ''

    def set_channel(self, channel_id):
        self.__topic_arn = channel_id
        return self

    def publish_msg(self, msg: str):
        if self.__topic_arn == '':
            raise ValueError('missing topic arn to publish message')
        response = self.__sns_client.publish(
            TopicArn=self.__topic_arn,
            # TargetArn='string',  # not needed coz of we are using topic arn
            # PhoneNumber='string',  # not needed coz of we are using topic arn
            Message=msg,
            # Subject='optional string',
            # MessageStructure='string',
            # MessageAttributes={
            #     'string': {
            #         'DataType': 'string',
            #         'StringValue': 'string',
            #         'BinaryValue': b'bytes'
            #     }
            # },
            # MessageDeduplicationId='string',
            # MessageGroupId='string'
        )
        return response

    def subscribe(self):
        raise NotImplemented('will implement it if needed.')
