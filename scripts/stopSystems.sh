#! /bin/sh

BASEDIR=$(dirname $0)

source $BASEDIR/config.sh


# start spark
$SPARK_HOME/sbin/stop-all.sh

# start zookeeper
$ZOOKEEPER_HOME/bin/zkServer.sh stop

# start HDFS
$HADOOP_HOME/sbin/stop-dfs.sh

jps
