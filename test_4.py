import boto3
from config import config
import boto3_helper
from pprint import pprint as pp
import threading
import time

# exitFlag = 0
#
#
# class MyThread(threading.Thread):
#     def __init__(self, thread_id, name, counter):
#         threading.Thread.__init__(self)
#         self.thread_id = thread_id
#         self.name = name
#         self.counter = counter
#
#     def run(self):
#         print("Starting " + self.name)
#         print_time(self.name, 5, self.counter)
#         print("Exiting " + self.name)
#
#
# def print_time(thread_name, counter, delay):
#     while counter:
#         if exitFlag:
#             thread_name.exit()
#         time.sleep(delay)
#         print("%s: %s" % (thread_name, time.ctime(time.time())))
#         counter -= 1
#
#
# # Create new threads
# thread1 = MyThread(1, "Thread-1", 1)
# thread2 = MyThread(2, "Thread-2", 2)
#
# # Start new Threads
# thread1.start()
# thread2.start()
#
# print("Exiting Main Thread")
#
# exit(1)
client = boto3.client(
    'dynamodb',
    endpoint_url=config['endpoint_url']
)

helper_client = boto3_helper.client(client)
table_name = 'employees'

# if True:
if False:
    from mysql.connector import MySQLConnection
    import mysql
    from time import time

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
            # 'AttributeType': 'S',
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

    sql_select_Query = '''
    select 
      emp_no,
      birth_date,
      first_name,
      last_name,
      gender,
      hire_date
    from
      employees 
    limit
      100000
    '''
    cursor = connection.cursor()
    cursor.execute(sql_select_Query)

    start = time()

    batch_items = []
    count = 0
    for row in cursor.fetchall():
        batch_items.append(
            {
                'PutRequest': {
                    'Item': {
                        'emp_no': {'N': str(row[0])},
                        # 'birth_date': {'S': str(row[1])},
                        'first_name': {'S': row[2]},
                        # 'last_name': {'S': row[3]},
                        # 'gender': {'S': str(row[4])},
                        # 'hire_date': {'S': str(row[5])}
                    }
                }
            }
        )
        if len(batch_items) == 25:
            helper_client.batch_write_item(RequestItems={table_name: batch_items})
            batch_items = []
            count = count + 25
            # print(count)

    if len(batch_items) > 0:
        helper_client.batch_write_item(RequestItems={table_name: batch_items})
        count = count + len(batch_items)

    print(f'Wrote {count} records in {int(time() - start)} seconds')
    # start = time()

if False:
    count = 0
    scanned_count = 0
    for response in helper_client.scan(
            Fields=[
                'ScannedCount',
                'Count',
                'ConsumedCapacity',
                # 'LastEvaluatedKey',
                'Duration'
            ],
            TableName=table_name,
            FilterExpression='begins_with(first_name, :first_name)',
            # FilterExpression='first_name=:first_name',
            ExpressionAttributeValues={
                ':first_name': {'S': 'A'}
                # ':first_name': {'S': 'Elzbieta'}
            },
            ReturnConsumedCapacity='TOTAL'
    ):
        count = count + response['Count']
        scanned_count = scanned_count + response['ScannedCount']
        pp(response)

    # print(response['Duration'])
    print(count)
    print(scanned_count)

pp(helper_client.get_item(
    Fields=[
        'ConsumedCapacity',
        'Duration',
        'Item'
    ],
    TableName=table_name,
    Key={
        'emp_no': {'N': '10001'},
        'first_name': {'S': 'Georgi'}
    },
    ReturnConsumedCapacity='TOTAL',
    ConsistentRead=False
))

pp(helper_client.query(
    Fields=[
        'ConsumedCapacity',
        'Duration',
        'Items'
    ],
    TableName=table_name,
    KeyConditionExpression='emp_no = :emp_no and begins_with(first_name,:first_name)',
    ExpressionAttributeValues={
        ':emp_no': {'N': '10001'},
        ':first_name': {'S': 'G'}
    },
    ReturnConsumedCapacity='TOTAL'
))
