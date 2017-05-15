#!/bin/bash
JAVA_OUT="./java/"
KOTLIN_OUT="./kotlin/"
mkdir -p $JAVA_OUT
mkdir -p $KOTLIN_OUT

protoc --java_out=$JAVA_OUT ./EventProtos.proto
java -jar kbuilders.jar --javaRoot=$JAVA_OUT --kotlinRoot=$KOTLIN_OUT