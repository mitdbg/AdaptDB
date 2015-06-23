#! /bin/bash

BASEDIR=$(dirname $0)
ABSPATH=$(cd "$BASEDIR"; pwd)
source $BASEDIR/config.sh

HDFS_DIR="/user/anil/data"
CODE_DIR="$ABSPATH/../code/mdindex"
OUTPUT_FILE="$ABSPATH/../test4"


# cleanup
#$BASEDIR/clean_partitions.sh $HDFS_DIR
#$BASEDIR/execOnAllNodes.py /usr/local/bin/drop_caches
#echo "cleanup done"

# build
echo "building ..."
cd $CODE_DIR
git pull
cd ant
ant clean
ant


# run
echo "running .."
nohup java -XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode -cp "$CODE_DIR/jar/mdindex.jar:$CODE_DIR/lib/*" core.access.benchmark.TestRepartitioning > $OUTPUT_FILE.out 2> $OUTPUT_FILE.err &

echo "done!"
