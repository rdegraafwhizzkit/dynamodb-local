# dynamodb-local

Test code to get to know DynamoDB without having to run it on AWS

## Requirements / tested with
* Python 3.7, tested with
  * Homebrew 3.7.2 on macOS Mojave
  * Custom compiled 3.7.7 on Debian 9.11
  * Windows 10
    * Git bash from PortableGit-2.25.1 (MINGW64_NT-10.0-18362)
    * WinPython 3.7.6.0
* Java JDK 1.8, tested with
  * Oracle 1.8.0_111 on macOS Mojave
  * OpenJDK 1.8.0_242 on Debian 9.11
  * Oracle 1.8.0_161 on Windows 10
* Maven 3, tested with
  * 3.5.2 on macOS Mojave
  * 3.3.9 on Debian 9.11
  * 3.6.0 on Windows 10

## In terminal 1: start a local DynamoDB server
```
cd java
./start-server.sh reset
```
- Omit the 'reset' parameter if you have already build the server before, it will be in the 'dynamodb' subdirectory.
- This will download necessary jar files, build the server and execute it. 
- You can browse to http://localhost:8000/shell/ to access a DynamoDB shell.
- Press ctrl-c to stop the server
- Data will persist upon reboot if you remove '-inMemory' from start-server.sh
- The server listens on 0.0.0.0, so it's accessible from the network

## In terminal 2: create a virtual environment, activate it and run code
On Windows:
```
alias python='winpty C:\Users\YOUR_USER_NAME\Desktop\Development\python-3.7.6.amd64\python.exe'
python -m venv venv
. venv/Scripts/activate
```
On macOS and Linux:
```
virtualenv -p python3.7 --no-site-packages venv
. venv/bin/activate
pip install --upgrade pip
```
Then
```
pip install --upgrade -r requirements.txt
cp config.py.template config.py
python test_1.py
python test_2.py
python test_3.py
```

## References

- https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html
- https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html

## Troubleshooting

When you run into botocore.exceptions.NoCredentialsError: Unable to locate credentials, perform

```
pip install --upgrade awscli
aws configure
AWS Access Key ID [None]: DummyAWSAccessKeyID     
AWS Secret Access Key [None]: DummyAWSSecretAccessKey
Default region name [None]: eu-west-1 
Default output format [None]: json 
``` 

Although you are running locally, boto still needs to read (dummy) credentials.

On Windows, you may also have to create an alias to python:

```
alias python='winpty C:\Users\YOUR_USER_NAME\Desktop\Development\python-3.7.6.amd64\python.exe'
```