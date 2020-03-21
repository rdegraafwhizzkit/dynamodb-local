#!/bin/bash

clear

if [ $# == 1 ] && [ "$1" == "reset" ]; then
  rm -rf dynamodb && mkdir dynamodb
  mvn -q clean package
  mvn dependency:build-classpath -Dmdep.outputFile=dynamodb/classpath
  cp -f target/*jar dynamodb/
  rm -rf target
fi

ARCH="win32-x64"
case $(uname) in
  Darwin)
    ARCH="osx"
    ;;
  Linux)
    ARCH="linux-amd64"
    ;;
esac

cd dynamodb || exit 1

echo Now browse to http://localhost:8000/shell/

java -cp "dynamodb-test-1.0-SNAPSHOT.jar:$(cat classpath)" \
  -Dsqlite4java.library.path="${HOME}/.m2/repository/com/almworks/sqlite4java/libsqlite4java-${ARCH}/1.0.392/" \
  LocalDynamoDBServer \
  -inMemory \
  -sharedDb
