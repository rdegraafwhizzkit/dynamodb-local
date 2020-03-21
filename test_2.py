import boto3
from pprint import pprint as pp
from helper import slice_dict
import boto3_helper
import random

client = boto3.client(
    'dynamodb',
    endpoint_url='http://localhost:8000'
)

helper_client = boto3_helper.client(client)

try:

    print(helper_client.delete_table(
        Check=True,
        TableName='test_2'
    ))

    response = helper_client.create_table(
        Delete=True,
        Check=True,
        AttributeDefinitions=[{
            'AttributeName': 'host',
            'AttributeType': 'S'
        }, {
            'AttributeName': 'timestamp',
            'AttributeType': 'N'
        }],
        TableName='test_2',
        KeySchema=[{
            'AttributeName': 'timestamp',
            'KeyType': 'HASH'
        }, {
            'AttributeName': 'host',
            'KeyType': 'RANGE'
        }],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
    pp(response['TableDescription'])
except client.exceptions.ResourceInUseException as error:
    pp(error)

i = 0
while i < 10000:
    response = client.put_item(
        TableName='test_2',
        Item={
            'host': {'S': 'Hello!'},
            'timestamp': {'N': str(i)},
            'rx_bytes': {'N': str(random.randint(0, 1000))}
        },
        ReturnConsumedCapacity='TOTAL'
    )
    i = i + 1
# pp(response['ConsumedCapacity'])

response = client.scan(
    TableName='test_2',
    ReturnConsumedCapacity='TOTAL'
)
pp(slice_dict(response.items(), ['ConsumedCapacity', 'Items']))

if False:
    response = client.query(
        TableName='test_2',
        KeyConditions={
            'host': {
                'AttributeValueList': [
                    {'S': 'Hello!'}
                ],
                'ComparisonOperator': 'EQ'
                # 'EQ' |'NE'|'IN'|'LE'|'LT'|'GE'|'GT'|'BETWEEN'|'NOT_NULL'|'NULL'|'CONTAINS'|'NOT_CONTAINS'|'BEGINS_WITH'
            }
        }
    )

    pp(slice_dict(response.items(), ['ConsumedCapacity', 'Items']))
