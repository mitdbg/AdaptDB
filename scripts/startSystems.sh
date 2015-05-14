#! /bin/sh

BASEDIR=$(dirname $0)

source $BASEDIR/config.sh

# start HDFS
$HADOOP_HOME/sbin/start-dfs.sh

# start zookeeper
$ZOOKEEPER_HOME/bin/zkServer.sh start

# start spark
$SPARK_HOME/sbin/start-all.sh

jps
