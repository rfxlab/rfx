#!/bin/bash

# where worker's jar to use (absolute path)
WORKER_JAR="$1"
WORKER_PARAMS="$2"

echo "jar path=$WORKER_JAR"

# where java to run
JAVA="/usr/lib/jvm/jdk1.7.0_51/bin/java"

# Memory options
if [ -z "$HEAP_OPTS" ]; then
  HEAP_OPTS="-Xmx2048M"
fi

# JVM performance options
if [ -z "$JVM_PERFORMANCE_OPTS" ]; then
  JVM_PERFORMANCE_OPTS="-server -XX:+UseCompressedOops -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -XX:+CMSScavengeBeforeRemark -XX:+DisableExplicitGC -Djava.awt.headless=true"
fi

# GC options
GC_FILE_SUFFIX='-gc.log'
GC_LOG_FILE_NAME=''
if [ "$1" = "daemon" ] && [ -z "$GC_LOG_OPTS"] ; then
  shift
  GC_LOG_FILE_NAME=$1$GC_FILE_SUFFIX
  shift
  GC_LOG_OPTS="-Xloggc:$LOG_DIR/$GC_LOG_FILE_NAME -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps "
fi

echo $JAVA $HEAP_OPTS $JVM_PERFORMANCE_OPTS $GC_LOG_OPTS -jar $WORKER_JAR $WORKER_PARAMS
nohup $JAVA $HEAP_OPTS $JVM_PERFORMANCE_OPTS $GC_LOG_OPTS -jar $WORKER_JAR $WORKER_PARAMS &
#rm -f nohup.out
