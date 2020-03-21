import boto3
from pprint import pprint as pp
from helper import slice_dict

client = boto3.client(
    'dynamodb',
    endpoint_url='http://localhost:8000'
)

try:
    response = client.create_table(
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
except client.exceptions.ResourceInUseException as error:
    pp(error)

client.get_waiter('table_exists').wait(TableName='test_1')

response = client.put_item(
    TableName='test_1',
    Item={'banaan': {'S': 'Hello!'}},
    ReturnConsumedCapacity='TOTAL'
)
pp(response['ConsumedCapacity'])

response = client.list_tables()
pp(response['TableNames'])

response = client.scan(
    TableName='test_1',
    ReturnConsumedCapacity='TOTAL'
)
pp(slice_dict(response.items(), ['ConsumedCapacity', 'Items']))

response = client.query(
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

pp(slice_dict(response.items(), ['ConsumedCapacity', 'Items']))
