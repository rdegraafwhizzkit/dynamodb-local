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

if True:
    # if False:
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
            'AttributeType': 'S'
        }, {
            'AttributeName': 'first_name',
            'AttributeType': 'S'
        }],
        ProvisionedThroughput={
            'ReadCapacityUnits': 2500,
            'WriteCapacityUnits': 2500
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
      1000000
    '''
    cursor = connection.cursor()
    cursor.execute(sql_select_Query)

    start = time()

    batch_items = []
    for row in cursor.fetchall():
        batch_items.append(
            {
                'PutRequest': {
                    'Item': {
                        'emp_no': {'S': str(row[0])},
                        'birth_date': {'S': str(row[1])},
                        'first_name': {'S': row[2]},
                        'last_name': {'S': row[3]},
                        'gender': {'S': str(row[4])},
                        'hire_date': {'S': str(row[5])}
                    }
                }
            }
        )
        if len(batch_items) == 25:
            client.batch_write_item(RequestItems={table_name: batch_items})
            batch_items = []

    if len(batch_items) > 0:
        client.batch_write_item(RequestItems={table_name: batch_items})

    print(time() - start)
    start = time()

count = 0
scanned_count = 0
for response in helper_client.scan(
        Fields=[
            'ScannedCount',
            'Count',
            # 'ConsumedCapacity',
            # 'LastEvaluatedKey',
            'Duration'
        ],
        TableName=table_name,
        # FilterExpression='begins_with(first_name, :first_name)',
        FilterExpression='first_name=:first_name',
        ExpressionAttributeValues={
            # ':first_name': {'S': 'F'}
            ':first_name': {'S': 'Elzbieta'}
        },
        ReturnConsumedCapacity='TOTAL'
):
    count = count + response['Count']
    scanned_count = scanned_count + response['ScannedCount']
    pp(response)

# print(response['Duration'])
print(count)
print(scanned_count)
