from pprint import pprint as pp
from boto3_helper import DynamoDBClient
from config import config
import boto3_helper

helper_client = boto3_helper.client(
    'dynamodb',
    endpoint_url=config['endpoint_url']
)

try:
    response = helper_client.create_table(
        AttributeDefinitions=[{
            'AttributeName': 'banaan',
            'AttributeType': 'S'  # |'N'|'B'
        }],
        TableName='test_1',
        KeySchema=[{
            'AttributeName': 'banaan',
            'KeyType': 'HASH'  # |'RANGE'
        }],
        ProvisionedThroughput={
            'ReadCapacityUnits': 123,
            'WriteCapacityUnits': 123
        }
    )
    pp(response['TableDescription'])
except helper_client.client.exceptions.ResourceInUseException as error:
    pp(error)

helper_client.get_waiter('table_exists').wait(TableName='test_1')

response = helper_client.put_item(
    TableName='test_1',
    Item={'banaan': {'S': 'Hello!'}},
    ReturnConsumedCapacity='TOTAL'
)
pp(response['ConsumedCapacity'])

response = helper_client.list_tables()
pp(response['TableNames'])

for response in helper_client.scan(
        TableName='test_1',
        ReturnConsumedCapacity='TOTAL',
        Fields=['ConsumedCapacity', 'Items', 'Duration']
):
    pp(response)

response = helper_client.query(
    TableName='test_1',
    KeyConditions={
        'banaan': {
            'AttributeValueList': [
                {'S': 'Hello!'}
            ],
            'ComparisonOperator': 'EQ'
            # 'EQ' |'NE'|'IN'|'LE'|'LT'|'GE'|'GT'|'BETWEEN'|'NOT_NULL'|'NULL'|'CONTAINS'|'NOT_CONTAINS'|'BEGINS_WITH'
        }
    }
)

pp(DynamoDBClient.slice(response.items(), ['ConsumedCapacity', 'Items']))
