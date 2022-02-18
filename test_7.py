from pprint import pprint as pp
import boto3_helper
import random
from config import config
import string
import uuid
import base64
import secrets
from batch_write_item import BatchWriteItemThread, BatchWriteItemThreadHelper

table_name = 'person_file_info'
dynamodb_client = boto3_helper.client(
    'dynamodb',
    endpoint_url=config['endpoint_url']
)

try:

    dynamodb_client.delete_table(
        Check=True,
        TableName=table_name
    )

    response = dynamodb_client.create_table(
        Delete=True,
        Check=True,
        AttributeDefinitions=[{
            'AttributeName': 'person_id',
            'AttributeType': 'S'
        }, {
            'AttributeName': 'document_id',
            'AttributeType': 'S'
        }],
        TableName=table_name,
        KeySchema=[{
            'AttributeName': 'person_id',
            'KeyType': 'HASH'
        }, {
            'AttributeName': 'document_id',
            'KeyType': 'RANGE'
        }],
        ProvisionedThroughput={
            'ReadCapacityUnits': 25,
            'WriteCapacityUnits': 25
        }
    )
except dynamodb_client.client.exceptions.ResourceInUseException as error:
    pp(error)

extensions = {
    0: '.pdf',
    1: '.doc',
    2: '.docx',
    3: '.jpeg',
    4: '.png'
}

# Keep at max (25) for AWS for optimal performance unless the items get too big (400KB per item, max 16MB per batch)
batch_size = 25

nr_threads = 4  # 11 secs

thread_helper = BatchWriteItemThreadHelper(nr_threads)

batch_items = []
i = 0
while i < 500000:
    item = {  # Around 300 bytes > 4 items in one WCU
        'person_id': {'S': ''.join(random.choices(string.hexdigits, k=24)).lower()},
        'document_id': {'S': str(uuid.uuid1())},
        'x_amz_key': {'S': base64.b64encode(secrets.token_bytes(48)).decode()},
        'x_amz_iv': {'S': base64.b64encode(secrets.token_bytes(16)).decode()},
        'extension': {'S': extensions.get((i % 5))},
        'done': {'N': '0'}
    }

    batch_items.append({'PutRequest': {'Item': item}})
    if len(batch_items) == batch_size:
        thread_helper.add(BatchWriteItemThread(dynamodb_client, table_name, batch_items))
        batch_items = []
    thread_helper.try_start()

    i += 1

if len(batch_items) > 0:
    thread_helper.add(BatchWriteItemThread(dynamodb_client, table_name, batch_items))
thread_helper.start()

print(f'Wrote {thread_helper.count} records in {thread_helper.duration / nr_threads} seconds')
print(f'Performance was {int(thread_helper.count * nr_threads / thread_helper.duration)} records/second')
print(f'Used {thread_helper.capacity_units} capacity units')

for response in dynamodb_client.scan(
        Fields=['ConsumedCapacity', 'Items', 'Count', 'ScannedCount'],
        TableName=table_name,
        ReturnConsumedCapacity='TOTAL'
):
    pp(response['Count'])
    pp(response['ConsumedCapacity'])
