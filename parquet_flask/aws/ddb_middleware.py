import decimal
import logging

import boto3
from boto3.dynamodb.conditions import Attr

from parquet_flask.aws.aws_cred import AwsCred

LOGGER = logging.getLogger(__name__)

VALID_KEY_TYPE = ['S', 'N', 'B']


class DDBMiddlewareProps:
    def __init__(self):
        self.__tbl_name = None
        self.__hash_key = None
        self.__range_key = None
        self.__hash_key_type = 'S'
        self.__range_key_type = 'S'

    @property
    def tbl_name(self):
        return self.__tbl_name

    @tbl_name.setter
    def tbl_name(self, val):
        """
        :param val: str
        :return: None
        """
        self.__tbl_name = val
        return

    @property
    def hash_key(self):
        return self.__hash_key

    @hash_key.setter
    def hash_key(self, val):
        """
        :param val: str
        :return: None
        """
        self.__hash_key = val
        return

    @property
    def range_key(self):
        return self.__range_key

    @range_key.setter
    def range_key(self, val):
        """
        :param val: str
        :return: None
        """
        self.__range_key = val
        return

    @property
    def hash_key_type(self):
        return self.__hash_key_type

    @hash_key_type.setter
    def hash_key_type(self, val):
        """
        :param val: str - 'S', 'N', or 'B'
        :return: None
        """
        if val not in VALID_KEY_TYPE:
            raise ValueError('input is not valid type. {} vs. {}'.format(val, VALID_KEY_TYPE))
        self.__hash_key_type = val
        return

    @property
    def range_key_type(self):
        return self.__range_key_type

    @range_key_type.setter
    def range_key_type(self, val):
        """
        :param val: str - 'S', 'N', or 'B'
        :return: None
        """
        if val not in VALID_KEY_TYPE:
            raise ValueError('input is not valid type. {} vs. {}'.format(val, VALID_KEY_TYPE))
        self.__range_key_type = val
        return


class DDBMiddleware(AwsCred):
    def __init__(self, props=DDBMiddlewareProps()):
        super().__init__()
        self.__props = props
        self._ddb_client = boto3.Session(**self.boto3_session).client('dynamodb')
        self._ddb_resource = boto3.Session(**self.boto3_session).resource('dynamodb')

    def has_table(self):
        if self.__props.tbl_name is None:
            raise ValueError('missing tbl_name')
        try:
            tbl_details = self._ddb_client().describe_table(TableName=self.__props.tbl_name)
            return tbl_details
        except Exception as e:
            # TODO should check if exception is this one "ResourceNotFoundException". if not, throw the error
            return None

    def create_table(self, gsi_list=[]):
        """
        ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.create_table
        :param gsi_list: list  - [{'IndexName': 'string','KeySchema': [{'AttributeName': 'string','KeyType': 'HASH'|'RANGE'},]}]
        :param primary_key: str - Hash Key
        :param secondary_key: str - Range Key (optional)
        :param primary_key_type: str - 'S', 'N', or 'B'
        :param secondary_key_type: str - 'S', 'N', or 'B'
        :return: dict - create table result
        """
        if self.__props.tbl_name is None:
            raise ValueError('missing tbl_name')
        if self.__props.hash_key is None:
            raise ValueError('missing hash_key')
        LOGGER.info('creating a table: {}'.format(self.__props.tbl_name))
        attribute_definitions = [
            {
                'AttributeName': self.__props.hash_key,
                'AttributeType': self.__props.hash_key_type,
            }
        ]
        key_schema = [
            {
                'AttributeName': self.__props.hash_key,
                'KeyType': 'HASH',  # 'RANGE' if there is secondary key
            }
        ]
        for each in gsi_list:
            each['Projection'] = {'ProjectionType': 'ALL'}
        if self.__props.range_key is not None:
            attribute_definitions.append({
                'AttributeName': self.__props.range_key,
                'AttributeType': self.__props.range_key_type,
            })
            key_schema.append({
                'AttributeName': self.__props.range_key,
                'KeyType': 'RANGE',
            })
        create_tbl_params = {
            'TableName': self.__props.tbl_name,
            'AttributeDefinitions': attribute_definitions,
            'KeySchema': key_schema,
            'BillingMode': 'PAY_PER_REQUEST',  # TODO setting it to on-demand. might need to re-visit later
            'SSESpecification': {'Enabled': False}  # TODO had to disable it since it does not support 'AES256' yet.
        }
        if len(gsi_list) > 0:
            create_tbl_params['GlobalSecondaryIndexes'] = gsi_list
        create_result = self._ddb_client().create_table(**create_tbl_params)
        return create_result

    def _replace_decimals(self, obj):
        """
        Ref:
            https://stackoverflow.com/a/46738251  in the comments
            https://github.com/boto/boto3/issues/369#issuecomment-157205696

        :param obj:
        :return:
        """
        if isinstance(obj, list):
            for i in range(len(obj)):
                obj[i] = self._replace_decimals(obj[i])
            return obj
        elif isinstance(obj, dict):
            for k in obj.keys():
                obj[k] = self._replace_decimals(obj[k])
            return obj
        elif isinstance(obj, decimal.Decimal):
            if obj % 1 == 0:
                return int(obj)
            else:
                return float(obj)
        else:
            return obj

    def get_one_item(self, hash_val, range_val=None):
        """
        retrieving a single item based on hash key
        :param hash_val:
        :param range_val:
        :return:
        """
        LOGGER.info('retrieving one item from DDB using they key')
        query_key = {self.__props.hash_key: hash_val}
        if range_val is not None and self.__props.range_key is not None:
            query_key[self.__props.range_key] = range_val
        item_result = self._ddb_resource().Table(self.__props.tbl_name).get_item(
            Key=query_key
        )
        if 'Item' not in item_result:
            return None
        return self._replace_decimals(item_result['Item'])

    def delete_one_item(self, hash_val, range_val=None):
        """

        Sample Response:
        {'Attributes': {...}}
        {'RequestId': '70PUK7HSNQI6VLHRM1Q7VPESJ3VV4KQNSO5AEMVJF66Q9ASUAAJG', 'HTTPStatusCode': 200, 'HTTPHeaders': {'server': 'Server', 'date': 'Mon, 08 Mar 2021 18:04:35 GMT', 'content-type': 'application/x-amz-json-1.0', 'content-length': '2', 'connection': 'keep-alive', 'x-amzn-requestid': '70PUK7HSNQI6VLHRM1Q7VPESJ3VV4KQNSO5AEMVJF66Q9ASUAAJG', 'x-amz-crc32': '2745614147'}, 'RetryAttempts': 0}

        :param hash_val:
        :param range_val:
        :return:
        """
        LOGGER.info('deleting one item from DDB using they key')
        query_key = {self.__props.hash_key: hash_val}
        if range_val is not None and self.__props.range_key is not None:
            query_key[self.__props.range_key] = range_val
        item_result = self._ddb_resource().Table(self.__props.tbl_name).delete_item(Key=query_key, ReturnValues='ALL_OLD')
        if 'Attributes' not in item_result:
            LOGGER.warning('cannot retrieved deleted attributes.')
            return None
        return item_result['Attributes']

    def add_one_item(self, item_dict, hash_val, range_val=None, replace=False):
        LOGGER.info('adding one item from DDB using they key')
        item_dict[self.__props.hash_key] = hash_val
        if range_val is not None and self.__props.range_key is not None:
            item_dict[self.__props.range_key] = range_val

        addition_arguments = {
            'Item': item_dict,
            'ReturnValues': 'ALL_OLD',
        }
        if replace is False:
            if range_val is not None and self.__props.range_key is not None:
                condition = Attr(self.__props.hash_key).eq(hash_val) and Attr(self.__props.range_key).ne(range_val)
            else:
                condition = '{} = {}'.format(self.__props.hash_key, hash_val)
            addition_arguments['ConditionExpression'] = condition
        item_result = self._ddb_resource().Table(self.__props.tbl_name).put_item(**addition_arguments)
        """
        {'ResponseMetadata': {'RequestId': '49876A3IFHPMRFIEUMANGFAO8VVV4KQNSO5AEMVJF66Q9ASUAAJG', 'HTTPStatusCode': 200, 'HTTPHeaders': {'server': 'Server', 'date': 'Mon, 08 Mar 2021 17:58:08 GMT', 'content-type': 'application/x-amz-json-1.0', 'content-length': '2', 'connection': 'keep-alive', 'x-amzn-requestid': '49876A3IFHPMRFIEUMANGFAO8VVV4KQNSO5AEMVJF66Q9ASUAAJG', 'x-amz-crc32': '2745614147'}, 'RetryAttempts': 0}}
        """
        # TODO check result
        return

    def scan_tbl(self, conditions_dict):
        LOGGER.info('scanning items from DDB using they key')
        current_tbl = self._ddb_resource().Table(self.__props.tbl_name)
        item_result = current_tbl.scan(
            Limit=1,
            ScanFilter=conditions_dict,
            Select='ALL_ATTRIBUTES')
        all_results = item_result['Items']
        while 'LastEvaluatedKey' in item_result and item_result['LastEvaluatedKey'] is not None:  # pagination
            item_result = current_tbl.scan(
                Limit=100,
                ScanFilter=conditions_dict,
                ExclusiveStartKey=item_result['LastEvaluatedKey'],
                Select='ALL_ATTRIBUTES')
            all_results.extend(item_result['Items'])
        return self._replace_decimals(all_results)

    def update_one_item(self, update_expression, expression_names, expression_vals, hash_val, range_val=None, retrieve_new_val=True):
        """
        Usage : increment or decrement

ddb.update_one_item('SET #created_at_key = #created_at_key + :created_at_val', {'#created_at_key': 'created_at'}, {':created_at_val': -50}, '61725b56-3016-42c6-9006-c0b5d9017fee')

        :param update_expression: str - example: add #created_key = :created_val
        :param expression_names: dict - {'#created_key': 'created_at'}
        :param expression_vals: dict - {':created_val': 123}
        :param hash_val: str
        :param range_val: str
        :return:
        """
        LOGGER.info('updating one item from DDB using they key')
        query_key = {self.__props.hash_key: hash_val}
        if range_val is not None and self.__props.range_key is not None:
            query_key[self.__props.range_key] = range_val
        item_result = self._ddb_resource().Table(self.__props.tbl_name).update_item(
            Key=query_key,
            UpdateExpression=update_expression,
            ExpressionAttributeNames=expression_names,
            ExpressionAttributeValues=expression_vals,
            ReturnValues='ALL_NEW' if retrieve_new_val is True else 'ALL_OLD'
        )
        if 'Attributes' not in item_result:
            return None
        return self._replace_decimals(item_result['Attributes'])
    pass
