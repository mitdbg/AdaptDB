#! /bin/bash

BASEDIR=$(dirname $0)

source $BASEDIR/config.sh

APP=$1

$SPARK_HOME/bin/spark-class org.apache.spark.deploy.Client kill spark://istc2.csail.mit.edu:7077 $APP
