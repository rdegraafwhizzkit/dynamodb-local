import boto3
from pprint import pprint as pp
import boto3_helper
from config import config
import json

helper_client = boto3_helper.client(
    'dynamodb',
    endpoint_url=config['endpoint_url']
)

try:

    helper_client.create_table(
        Delete=True,
        Check=True,
        TableName='test_3',
        KeySchema=[{
            'AttributeName': 'pattern_name',
            'KeyType': 'HASH'
        }, {
            'AttributeName': 'category',
            'KeyType': 'RANGE'
        }],
        AttributeDefinitions=[{
            'AttributeName': 'pattern_name',
            'AttributeType': 'S'
        }, {
            'AttributeName': 'category',
            'AttributeType': 'S'
        }],
        ProvisionedThroughput={
            'ReadCapacityUnits': 25,
            'WriteCapacityUnits': 25
        }
    )
except helper_client.client.exceptions.ResourceInUseException as error:
    pp(error)

pattern = [
    {'colors': ['red'], 'duration': 4000},
    {'colors': ['yellow'], 'duration': 1000},
    {'colors': ['green'], 'duration': 8000}
]

helper_client.put_item(
    TableName='test_3',
    Item={
        'pattern_name': {'S': 'Stoplicht'},
        'category': {'S': 'traffic#lights'},
        'pattern': {'S': json.dumps(pattern, separators=(',', ':'))}
    },
    ReturnConsumedCapacity='TOTAL'
)

helper_client.put_item(
    TableName='test_3',
    Item={
        'pattern_name': {'S': 'Stoplicht2'},
        'category': {'S': 'traffic#lights'},
        'pattern': {'S': json.dumps(pattern, separators=(',', ':'))}
    },
    ReturnConsumedCapacity='TOTAL'
)

helper_client.put_item(
    TableName='test_3',
    Item={
        'pattern_name': {'S': 'Politie auto'},
        'category': {'S': 'traffic#vehicles'},
        'pattern': {'S': json.dumps({}, separators=(',', ':'))}
    },
    ReturnConsumedCapacity='TOTAL'
)

pp(helper_client.query(
    Fields=['ScannedCount', 'Count', 'Items'],
    TableName='test_3',
    KeyConditionExpression='pattern_name = :pattern_name',
    ExpressionAttributeValues={
        ':pattern_name': {'S': 'Stoplicht'}
    }
))

for response in helper_client.scan(
        Fields=['ScannedCount', 'Count', 'Items', 'ConsumedCapacity'],
        TableName='test_3',
        FilterExpression='begins_with(category, :category)',
        ExpressionAttributeValues={
            ':category': {'S': 'traffic#vehicles'}
        },
        ReturnConsumedCapacity='TOTAL'
):
    pp(response)
