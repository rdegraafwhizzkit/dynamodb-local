import mysql
from mysql.connector import MySQLConnection
from config import config
import boto3_helper
from batch_write_item import BatchWriteItemThread, BatchWriteItemThreadHelper
from mysql_helper import resultset_iterator
from pprint import pprint as pp

connection = mysql.connector.connect(
    host=config['host'],
    database=config['database'],
    user=config['user'],
    password=config['password']
)

sql_select_query = 'select emp_no, birth_date, first_name, last_name, gender, hire_date from employees limit 1000000'

cursor = connection.cursor()
cursor.execute(sql_select_query)

helper_client = boto3_helper.client(
    'dynamodb',
    endpoint_url=config['endpoint_url']
)
table_name = 'employees'

helper_client.create_table(
    Delete=True,
    Check=True,
    TableName=table_name,
    KeySchema=[{
        'AttributeName': 'emp_no',
        'KeyType': 'HASH'
    }, {
        'AttributeName': 'first_name',
        'KeyType': 'RANGE'
    }],
    AttributeDefinitions=[{
        'AttributeName': 'emp_no',
        'AttributeType': 'N'
    }, {
        'AttributeName': 'first_name',
        'AttributeType': 'S'
    }, {
        'AttributeName': 'gender',
        'AttributeType': 'S'
    }],
    ProvisionedThroughput={
        'ReadCapacityUnits': 50,
        'WriteCapacityUnits': 50
    },
    GlobalSecondaryIndexes=[
        {
            'IndexName': f'{table_name}_idx1',  # Every GSI makes the put_items 'twice' as expensive
            'KeySchema': [
                {
                    'AttributeName': 'gender',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'emp_no',
                    'KeyType': 'RANGE'
                }
            ],
            'Projection': {
                'ProjectionType': 'ALL'
            },
            'ProvisionedThroughput': {
                'ReadCapacityUnits': 50,
                'WriteCapacityUnits': 50
            }
        },
    ],
)

# Keep at max (25) for AWS for optimal performance unless the items get too big (400KB per item, max 16MB per batch)
batch_size = 25

# Fetch size does not seem to matter too much
fetch_size = 5000

nr_threads = 4  # 11 secs

thread_helper = BatchWriteItemThreadHelper(nr_threads)

batch_items = []
for row in resultset_iterator(cursor, fetch_size):

    batch_items.append({'PutRequest': {'Item': {
        'emp_no': {'N': str(row[0])},
        'birth_date': {'S': str(row[1])},
        'first_name': {'S': row[2]},
        'last_name': {'S': row[3]},
        'gender': {'S': str(row[4])},
        'hire_date': {'S': str(row[5])},
    }}})

    if len(batch_items) == batch_size:
        thread_helper.add(BatchWriteItemThread(helper_client, table_name, batch_items))
        batch_items = []
    thread_helper.try_start()

if len(batch_items) > 0:
    thread_helper.add(BatchWriteItemThread(helper_client, table_name, batch_items))
thread_helper.start()

print(f'Wrote {thread_helper.count} records in {thread_helper.duration / nr_threads} seconds')
print(f'Performance was {int(thread_helper.count * nr_threads / thread_helper.duration)} records/second')
print(f'Used {thread_helper.capacity_units} capacity units')

pp(helper_client.query(
    Fields=['ScannedCount', 'Count', 'Items', 'Duration', 'ConsumedCapacity'],
    TableName=f'{table_name}',
    IndexName=f'{table_name}_idx1',
    KeyConditionExpression='gender = :gender',
    FilterExpression='begins_with(birth_date, :birth_date)',
    ExpressionAttributeValues={
        ':gender': {'S': 'M'},
        ':birth_date': {'S': '1965-01'}
    },
    ReturnConsumedCapacity='TOTAL'
))
