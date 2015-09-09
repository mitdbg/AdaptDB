#!/bin/bash

BASEDIR=$(dirname $0)

source $BASEDIR/config.sh

# start HDFS
$HADOOP_HOME/sbin/start-dfs.sh

# start spark
$SPARK_HOME/sbin/start-all.sh

jps
