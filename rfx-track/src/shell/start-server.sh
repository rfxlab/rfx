#!/bin/sh

JVM_PARAMS="-Xms512m -Xmx3g -XX:+TieredCompilation -XX:+UseCompressedOops -XX:+DisableExplicitGC -XX:+UseNUMA -XX:MaxPermSize=3g -server -XX:+UseConcMarkSweepGC"
JAR_NAME="click-track-server-1.0.jar"

/usr/bin/java7 -jar $JVM_PARAMS $JAR_NAME 127.0.0.1 8080 >> /dev/null 2>&1 &