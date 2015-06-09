#! /bin/sh

HADOOP_HOME="/Users/alekh/Softwares/sources/Hadoop/hadoop-2.6.0"

BASE_DIR=$1
START=$2
END=$3

for (( i=$START; i<=$END; i++ ))
do
  $HADOOP_HOME/bin/hadoop fs -rm $BASE_DIR/$i
done

echo "done"
