#!/bin/bash

BASEDIR=$(dirname $0)

source $BASEDIR/config.sh

# start zookeeper
$ZOOKEEPER_HOME/bin/zkServer.sh start

jps
