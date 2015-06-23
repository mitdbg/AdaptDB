#! /bin/bash

BASEDIR=$(dirname $0)

source $BASEDIR/config.sh


$BASEDIR/execOnAllNodes.py /usr/local/bin/drop_caches
