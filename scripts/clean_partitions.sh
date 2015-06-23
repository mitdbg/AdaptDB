#! /bin/bash

#HADOOP_HOME="/Users/alekh/Softwares/sources/Hadoop/hadoop-2.6.0"

BASEDIR=$(dirname $0)

source $BASEDIR/config.sh

BASE_DIR=$1


$HADOOP_HOME/bin/hadoop fs -rm $BASE_DIR/partitions[0-9]/819[2-9]
$HADOOP_HOME/bin/hadoop fs -rm $BASE_DIR/partitions[0-9]/8[2-9][0-9][0-9]
$HADOOP_HOME/bin/hadoop fs -rm $BASE_DIR/partitions[0-9]/9[0-9][0-9][0-9]
$HADOOP_HOME/bin/hadoop fs -rm $BASE_DIR/partitions[0-9]/[1-9][0-9][0-9][0-9][0-9]

$HADOOP_HOME/bin/hadoop fs -rm $BASE_DIR/repartition/*

echo "done"
