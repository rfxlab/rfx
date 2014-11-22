#!/bin/bash

# where java to run
JAVA="/usr/lib/jvm/jdk1.7.0_67/bin/java"

echo $JAVA -jar supervisor-starter.jar
nohup $JAVA -jar supervisor-starter.jar &

sleep 5

echo $JAVA -jar master-starter.jar
nohup $JAVA -jar master-starter.jar &

sleep 5

echo $JAVA -jar scheduled-job-starter.jar
nohup $JAVA -jar scheduled-job-starter.jar &