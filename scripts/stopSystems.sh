#!/bin/bash

BASEDIR=$(dirname $0)

source $BASEDIR/config.sh

# stop spark
$SPARK_HOME/sbin/stop-all.sh

# stop HDFS
$HADOOP_HOME/sbin/stop-dfs.sh

jps
