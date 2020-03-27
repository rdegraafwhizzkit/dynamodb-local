import boto3
from config import config
import boto3_helper
from pprint import pprint as pp

client = boto3.client(
    'dynamodb',
    endpoint_url=config['endpoint_url']
)

helper_client = boto3_helper.client(client)
table_name = 'employees'

parameters = {
    'reload': False,
    'scan': False or True
}

if parameters['reload']:
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
      10000
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
                    # Less fields in the item means less KB to scan/write so less RCU/WCU used
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

if parameters['scan']:
    count = 0
    scanned_count = 0
    duration = 0
    for response in helper_client.scan(
            Fields=[
                'ScannedCount',
                'Count',
                'ConsumedCapacity',
                'Duration'
            ],
            TableName=table_name,
            FilterExpression='begins_with(first_name, :first_name)',  # Does not make the scan cheaper
            ExpressionAttributeValues={':first_name': {'S': 'A'}},
            ReturnConsumedCapacity='TOTAL',
            ConsistentRead=True  # Makes it twice as expensive
    ):
        count = count + response['Count']
        scanned_count = scanned_count + response['ScannedCount']
        duration = response['Duration']
        pp(response)

    print(duration)
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
    ReturnConsumedCapacity='TOTAL',
    ConsistentRead=True  # Makes it twice as expensive
))
