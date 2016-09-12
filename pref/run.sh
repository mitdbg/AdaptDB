#!/bin/sh

~/Documents/workspace/spark-1.6.2-bin-hadoop2.6//bin/spark-submit --class pref_workload --executor-memory 6G --master "local[2]" target/scala-2.10/pref_workload-assembly-1.0.jar

# ~/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class pref_workload --executor-memory 200G --executor-cores 6 --master spark://istc13.csail.mit.edu:7077 ~/pref_workload-assembly-1.0.jar distinct 
