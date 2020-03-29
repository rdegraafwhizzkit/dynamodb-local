import mysql
from mysql.connector import MySQLConnection
from config import config
import boto3_helper
from batch_write_item import BatchWriteItemThread, BatchWriteItemThreadHelper
from mysql_helper import resultset_iterator

connection = mysql.connector.connect(
    host=config['host'],
    database=config['database'],
    user=config['user'],
    password=config['password']
)

sql_select_query = 'select emp_no, first_name from employees limit 1023'

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
    }],
    ProvisionedThroughput={
        'ReadCapacityUnits': 50,
        'WriteCapacityUnits': 50
    }
)

# Keep at max (25) for AWS for optimal performance unless the items get too big (400KB per item, max 16MB per batch)
batch_size = 25

# Fetch size does not seem to matter too much
fetch_size = 5000

# nr_threads = 1  # 20 secs
# nr_threads = 2  # 14 secs
nr_threads = 4  # 11 secs

thread_helper = BatchWriteItemThreadHelper(nr_threads)

batch_items = []
for row in resultset_iterator(cursor, fetch_size):

    batch_items.append({'PutRequest': {'Item': {
        'emp_no': {'N': str(row[0])},
        'first_name': {'S': row[1]},
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
