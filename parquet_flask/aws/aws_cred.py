from parquet_flask.utils.config import Config


class AwsCred:
    def __init__(self):
        self.__boto3_session = {
            'aws_access_key_id': Config().get_value('aws_access_key_id'),
            'aws_secret_access_key': Config().get_value('aws_secret_access_key'),
            'region_name': 'us-west-2',
        }
        aws_session_token = Config().get_value('aws_session_token')
        if aws_session_token is not None:
            self.__boto3_session['aws_session_token'] = aws_session_token

    @property
    def boto3_session(self):
        return self.__boto3_session

    @boto3_session.setter
    def boto3_session(self, val):
        """
        :param val:
        :return: None
        """
        self.__boto3_session = val
        return
