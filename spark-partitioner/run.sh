#!/bin/sh

~/Documents/workspace/spark-1.6.2-bin-hadoop2.6/bin/spark-submit --class spark_partitioner --executor-memory 6G --executor-cores 2 --master "local[2]" target/scala-2.10/spark-partitioner-assembly-1.0.jar hdfs://localhost:9000/user/ylu/tpch1-spark/part hdfs://localhost:9000/user/ylu/tpch1/part
