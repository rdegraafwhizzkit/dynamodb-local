import re


def is_instance(o: object, t: str):
    return re.sub(r'^.*\'(.*)\'.*$', r'\1', str(type(o))) == t


def client(client):
    if is_instance(client, 'botocore.client.DynamoDB'):
        return DynamoDBClient(client)
    raise Exception(f'{str(type(client))} is not supported')


class DynamoDBClient:

    def __init__(self, client):
        self.client = client

    @staticmethod
    def slice(dictionary: dict, keys: list):
        return {k: v for k, v in dictionary if k in keys}

    def table_exists(self, TableName):
        for page in self.client.get_paginator('list_tables').paginate():
            if TableName in page['TableNames']:
                return True
        return False

    # {'TableDescription': {'AttributeDefinitions': [{'AttributeName': 'host', 'AttributeType': 'S'}, {'AttributeName': 'timestamp', 'AttributeType': 'N'}], 'TableName': 'test_2', 'KeySchema': [{'AttributeName': 'timestamp', 'KeyType': 'HASH'}, {'AttributeName': 'host', 'KeyType': 'RANGE'}], 'TableStatus': 'ACTIVE', 'CreationDateTime': datetime.datetime(2020, 3, 20, 22, 25, 50, 956000, tzinfo=tzlocal()), 'ProvisionedThroughput': {'LastIncreaseDateTime': datetime.datetime(1970, 1, 1, 1, 0, tzinfo=tzlocal()), 'LastDecreaseDateTime': datetime.datetime(1970, 1, 1, 1, 0, tzinfo=tzlocal()), 'NumberOfDecreasesToday': 0, 'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}, 'TableSizeBytes': 15975, 'ItemCount': 489, 'TableArn': 'arn:aws:dynamodb:ddblocal:000000000000:table/test_2'}, 'ResponseMetadata': {'RequestId': 'b745f22a-3619-4aa4-834a-4bee4213d256', 'HTTPStatusCode': 200, 'HTTPHeaders': {'content-type': 'application/x-amz-json-1.0', 'x-amz-crc32': '2209538279', 'x-amzn-requestid': 'b745f22a-3619-4aa4-834a-4bee4213d256', 'content-length': '584', 'server': 'Jetty(8.1.12.v20130726)'}, 'RetryAttempts': 0}}
    def delete_table(self, **kwargs):
        if not kwargs.pop('Check', False) or self.table_exists(TableName=kwargs['TableName']):
            response = self.client.delete_table(**kwargs)
            self.client.get_waiter('table_not_exists').wait(TableName=kwargs['TableName'])
            return response
        return {
            'HTTPStatusCode': 404
        }

    def create_table(self, **kwargs):
        if kwargs.pop('Delete', False):
            self.delete_table(
                Check=kwargs.pop('Check', False),
                TableName=kwargs['TableName']
            )
        response = self.client.create_table(**kwargs)
        self.client.get_waiter('table_exists').wait(TableName=kwargs['TableName'])
        return response

    def put_item(self, **kwargs):
        exists_ok = kwargs.pop('ExistsOK', False)
        try:
            self.client.put_item(**kwargs)
        except self.client.exceptions.ConditionalCheckFailedException as e:
            if not exists_ok:
                raise e

    def query(self, **kwargs):
        fields = kwargs.pop('Fields', None)
        response = self.client.query(**kwargs)
        return response if fields is None else DynamoDBClient.slice(response.items(), fields)

    def scan(self, **kwargs):
        fields = kwargs.pop('Fields', None)
        response = self.client.scan(**kwargs)
        return response if fields is None else DynamoDBClient.slice(response.items(), fields)
