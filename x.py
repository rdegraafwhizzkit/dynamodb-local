import boto3
from pprint import pprint as pp

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
        TableName='test',
        KeySchema=[{
            'AttributeName': 'banaan',
            'KeyType': 'HASH'  # |'RANGE'
        }],
        ProvisionedThroughput={
            'ReadCapacityUnits': 123,
            'WriteCapacityUnits': 123
        }
    )
    pp(response)
except client.exceptions.ResourceInUseException as error:
    pass

client.get_waiter('table_exists').wait(TableName='test')

response = client.put_item(
    TableName='test',
    Item={
        'banaan': {
            'S': 'Hello!'
        }
    },
    ReturnConsumedCapacity='TOTAL'
)
pp(response)

response = client.list_tables()
pp(response)

response = client.scan(
    TableName='test',
    ReturnConsumedCapacity='TOTAL'
)
pp(response)
