import boto3
import mysql
from mysql.connector import MySQLConnection
from config import config
import boto3_helper
from batch_write_item import BatchWriteItem
from pprint import pprint as pp


def resultset_iterator(cursor, fetch_size=1000):
    while True:
        results = cursor.fetchmany(fetch_size)
        if not results:
            break
        for result in results:
            yield result


client = boto3.client(
    'dynamodb',
    endpoint_url=config['endpoint_url']
)

helper_client = boto3_helper.client(client)
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

connection = mysql.connector.connect(
    host=config['host'],
    database=config['database'],
    user=config['user'],
    password=config['password']
)

sql_select_query = 'select emp_no, first_name from employees limit 1000000'

cursor = connection.cursor()
cursor.execute(sql_select_query)

duration = 0
batch_items = []
batch_size = 25  # Keep at max for AWS for optimal performance
fetch_size = 5000
count = 0
threads = []
capacity_units = 0
# nr_threads = 1  # 20 secs
# nr_threads = 2  # 14 secs
nr_threads = 4  # 11 secs
for row in resultset_iterator(cursor, fetch_size):  # Batch size does not seem to matter too much

    batch_items.append({'PutRequest': {'Item': {
        'emp_no': {'N': str(row[0])},
        'first_name': {'S': row[1]},
    }}})

    if len(batch_items) == batch_size:
        threads.append(BatchWriteItem(helper_client, table_name, batch_items))
        batch_items = []

    if len(threads) == nr_threads:
        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()
            duration = duration + thread.response['Duration']
            count = count + thread.count
            capacity_units = capacity_units + thread.response['ConsumedCapacity'][0]['CapacityUnits']

        threads = []

if len(batch_items) > 0:
    threads.append(BatchWriteItem(helper_client, table_name, batch_items))

for thread in threads:
    thread.start()

for thread in threads:
    thread.join()
    duration = duration + thread.response['Duration']
    count = count + thread.count

print(f'Wrote {count} records in {duration / nr_threads} seconds: {int(count * nr_threads / duration)} records/second.')
print(f'Used {capacity_units} capacity units')
