# dynamodb-local

Test code to get to know DynamoDB without having to run it on AWS

## Requirements
- Python 3.7 (tested with 3.7.2)
- Java JDK (tested with Oracle 1.8.0_111 on macOS Mojave)
- Maven (tested with 3.5.2)

## In terminal 1: start a local DynamoDB server
```
cd java
./start-server.sh reset
```
- Omit the 'reset' parameter if you have already build the server before, it will be in the 'dynamodb' subdirectory.
- This will download necessary jar files, build the server and execute it. 
- You can browse to http://localhost:8000/shell/ to access a DynamoDB shell.
- Press ctrl-c to stop the server
- Data will persist upon reboot

## In terminal 2: create a virtual environment, activate it and run code
```
virtualenv -p python3 --no-site-packages venv-mac
. venv-mac/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
python test_1.py
```

## References

- https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html
- https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html
