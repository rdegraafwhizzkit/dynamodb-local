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
SEP=";"
S4JLP=$(echo ${HOME}|awk '{x=gensub(/^\/([a-z])\/(.*)$/,"\\1:\\\\\\2","g",$1);print(gensub(/\//,"\\\\","g",x))}')"\.m2\repository\com\almworks\sqlite4java\sqlite4java-win32-x64\1.0.392"

case $(uname) in
  Darwin)
    ARCH="osx"
    SEP=":"
    S4JLP="${HOME}/.m2/repository/com/almworks/sqlite4java/libsqlite4java-${ARCH}/1.0.392/"
    ;;
  Linux)
    ARCH="linux-amd64"
    SEP=":"
    S4JLP="${HOME}/.m2/repository/com/almworks/sqlite4java/libsqlite4java-${ARCH}/1.0.392/"
    ;;
esac

cd dynamodb || exit 1

echo Now browse to http://localhost:8000/shell/

java -cp "dynamodb-test-1.0-SNAPSHOT.jar${SEP}$(cat classpath)" \
  -Dsqlite4java.library.path=${S4JLP} \
  LocalDynamoDBServer \
  -inMemory \
  -sharedDb
