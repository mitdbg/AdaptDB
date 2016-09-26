#!/bin/sh

~/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class pref_workload --executor-memory 200G --executor-cores 4 --master spark://istc13.csail.mit.edu:7077 ~/AdaptDB/pref/target/scala-2.10/pref_workload-assembly-1.0.jar distinct

~/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class pref_workload --executor-memory 200G --executor-cores 4 --master spark://istc13.csail.mit.edu:7077 ~/AdaptDB/pref/target/scala-2.10/pref_workload-assembly-1.0.jar tpch3
~/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class pref_workload --executor-memory 200G --executor-cores 4 --master spark://istc13.csail.mit.edu:7077 ~/AdaptDB/pref/target/scala-2.10/pref_workload-assembly-1.0.jar tpch5
~/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class pref_workload --executor-memory 200G --executor-cores 4 --master spark://istc13.csail.mit.edu:7077 ~/AdaptDB/pref/target/scala-2.10/pref_workload-assembly-1.0.jar tpch6
~/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class pref_workload --executor-memory 200G --executor-cores 4 --master spark://istc13.csail.mit.edu:7077 ~/AdaptDB/pref/target/scala-2.10/pref_workload-assembly-1.0.jar tpch8
~/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class pref_workload --executor-memory 200G --executor-cores 4 --master spark://istc13.csail.mit.edu:7077 ~/AdaptDB/pref/target/scala-2.10/pref_workload-assembly-1.0.jar tpch10
~/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class pref_workload --executor-memory 200G --executor-cores 4 --master spark://istc13.csail.mit.edu:7077 ~/AdaptDB/pref/target/scala-2.10/pref_workload-assembly-1.0.jar tpch12
~/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class pref_workload --executor-memory 200G --executor-cores 4 --master spark://istc13.csail.mit.edu:7077 ~/AdaptDB/pref/target/scala-2.10/pref_workload-assembly-1.0.jar tpch14
~/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class pref_workload --executor-memory 200G --executor-cores 4 --master spark://istc13.csail.mit.edu:7077 ~/AdaptDB/pref/target/scala-2.10/pref_workload-assembly-1.0.jar tpch19
