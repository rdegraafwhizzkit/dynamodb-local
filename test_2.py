import boto3
from pprint import pprint as pp
import boto3_helper
import random
from config import config

# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#client
client = boto3.client(
    'dynamodb',
    endpoint_url=config['endpoint_url']
)

helper_client = boto3_helper.client(
    'dynamodb',
    endpoint_url=config['endpoint_url']
)

try:

    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.delete_table
    helper_client.delete_table(
        Check=True,
        TableName='test_2'
    )

    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.create_table
    response = helper_client.create_table(
        Delete=True,
        Check=True,
        AttributeDefinitions=[{
            'AttributeName': 'host_sk',
            'AttributeType': 'S'
        }, {
            'AttributeName': 'timestamp_pk',
            'AttributeType': 'N'
        }],
        TableName='test_2',
        KeySchema=[{
            'AttributeName': 'timestamp_pk',
            'KeyType': 'HASH'
        }, {
            'AttributeName': 'host_sk',
            'KeyType': 'RANGE'
        }],
        ProvisionedThroughput={
            'ReadCapacityUnits': 25,
            'WriteCapacityUnits': 25
        }
    )
except client.exceptions.ResourceInUseException as error:
    pp(error)

hosts = {
    0: 'mapy.cz',
    1: 'who.int',
    2: 'wikia.com',
    3: 'over-blog.com',
    4: 'phoca.cz'
}

i = 0
while i < 50:
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.put_item
    helper_client.put_item(
        ExistsOK=False,
        TableName='test_2',
        Item={
            'timestamp_pk': {'N': str(int(i / 5))},
            'host_sk': {'S': hosts.get((i % 5))},
            'rx_bytes': {'N': str(random.randint(0, 1000))}
        },
        ReturnConsumedCapacity='TOTAL',
        ConditionExpression='attribute_not_exists(timestamp_pk)'
    )
    i = i + 1

# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.scan
for response in helper_client.scan(
        Fields=['ConsumedCapacity', 'Items', 'Count', 'ScannedCount'],
        TableName='test_2',
        ReturnConsumedCapacity='TOTAL'
):
    pp(response)

# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.query
pp(helper_client.query(
    Fields=['ScannedCount', 'Count', 'Items'],
    TableName='test_2',
    KeyConditions={
        # 'host_sk': {
        #     'AttributeValueList': [
        #         {'S': 'who.int'}
        #     ],
        #     'ComparisonOperator': 'EQ'
        # },
        'timestamp_pk': {
            'AttributeValueList': [
                {'N': '2'},
            ],
            'ComparisonOperator': 'EQ'
        }
    },
    FilterExpression='rx_bytes >= :rx_min',
    ExpressionAttributeValues={
        ':rx_min': {'N': '500'}
    }
))
