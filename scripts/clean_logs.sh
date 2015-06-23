#! /bin/bash

BASEDIR=$(dirname $0)

source $BASEDIR/config.sh


$BASEDIR/execOnAllNodes.py "rm -rf /tmp/*"
$BASEDIR/execOnAllNodes.py "rm -rf /data/mdindex/tmp/*"
$BASEDIR/execOnAllNodes.py "rm -rf /data/mdindex/iotmp/*"
$BASEDIR/execOnAllNodes.py "rm -rf /home/mdindex/hadoop-2.6.0/logs/*"
$BASEDIR/execOnAllNodes.py "rm -rf /home/mdindex/spark-1.3.1-bin-hadoop2.6/logs/*"
$BASEDIR/execOnAllNodes.py "rm -rf /home/mdindex/spark-1.3.1-bin-hadoop2.6/work/*"

rm -rf zookeeper/data/zk1/version*
rm -rf zookeeper/log/zk1/version*
rm  zookeeper/data/zk1/zookeeper_server*
rm zookeeper.out
