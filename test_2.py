from pprint import pprint as pp
import boto3_helper
import random
from config import config

helper_client = boto3_helper.client(
    'dynamodb',
    endpoint_url=config['endpoint_url']
)

try:

    helper_client.delete_table(
        Check=True,
        TableName='test_2'
    )

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
except helper_client.client.exceptions.ResourceInUseException as error:
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

for response in helper_client.scan(
        Fields=['ConsumedCapacity', 'Items', 'Count', 'ScannedCount'],
        TableName='test_2',
        ReturnConsumedCapacity='TOTAL'
):
    pp(response)

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
