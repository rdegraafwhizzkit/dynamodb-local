import re
from time import time
import boto3


def is_instance(o: object, t: str):
    return re.sub(r'^.*\'(.*)\'.*$', r'\1', str(type(o))) == t


# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#client
def client(*args, **kwargs):
    botocore_client = boto3.client(*args, **kwargs)
    if is_instance(botocore_client, 'botocore.client.DynamoDB'):
        return DynamoDBClient(botocore_client)
    print(f'{str(type(botocore_client))} is not supported')
    return botocore_client


class DynamoDBClient:

    def __init__(self, dynamodb_client):
        self.client = dynamodb_client

    @staticmethod
    def slice(dictionary: dict, keys: list):
        return {k: v for k, v in dictionary if k in keys}

    def table_exists(self, **kwargs):
        table_name = kwargs.pop('TableName')
        for page in self.client.get_paginator('list_tables').paginate():
            if table_name in page['TableNames']:
                return True
        return False

    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.delete_table
    def delete_table(self, **kwargs):
        if not kwargs.pop('Check', False) or self.table_exists(TableName=kwargs['TableName']):
            response = self.client.delete_table(**kwargs)
            self.client.get_waiter('table_not_exists').wait(TableName=kwargs['TableName'])
            return response
        return {
            'HTTPStatusCode': 404
        }

    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.create_table
    def create_table(self, **kwargs):
        if kwargs.pop('Delete', False):
            self.delete_table(
                Check=kwargs.pop('Check', False),
                TableName=kwargs['TableName']
            )
        response = self.client.create_table(**kwargs)
        self.client.get_waiter('table_exists').wait(TableName=kwargs['TableName'])
        return response

    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.put_item
    def put_item(self, **kwargs):
        exists_ok = kwargs.pop('ExistsOK', False)
        try:
            return self.client.put_item(**kwargs)
        except self.client.exceptions.ConditionalCheckFailedException as e:
            if not exists_ok:
                raise e

    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.query
    def query(self, **kwargs):
        fields = kwargs.pop('Fields', None)
        start = time()
        response = self.client.query(**kwargs)
        response['Duration'] = time() - start
        return response if fields is None else DynamoDBClient.slice(response.items(), fields)

    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.scan
    def scan(self, **kwargs):
        fields = kwargs.pop('Fields', None)
        start = time()
        response = None
        while True:
            response = self.client.scan(
                **kwargs
            ) if response is None else self.client.scan(
                **kwargs,
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            response['Duration'] = time() - start
            yield response if fields is None else DynamoDBClient.slice(response.items(), fields)
            if 'LastEvaluatedKey' not in response:
                break

    def batch_write_item(self, **kwargs):
        fields = kwargs.pop('Fields', None)
        start = time()
        response = self.client.batch_write_item(**kwargs)
        response['Duration'] = time() - start
        return response if fields is None else DynamoDBClient.slice(response.items(), fields)

    def get_item(self, **kwargs):
        fields = kwargs.pop('Fields', None)
        start = time()
        response = self.client.get_item(**kwargs)
        response['Duration'] = time() - start
        return response if fields is None else DynamoDBClient.slice(response.items(), fields)

    def get_waiter(self, waiter_name):
        return self.client.get_waiter(waiter_name)

    def list_tables(self, **kwargs):
        return self.client.list_tables(**kwargs)
