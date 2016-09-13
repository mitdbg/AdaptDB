#!/bin/sh

# ~/Documents/workspace/spark-1.6.2-bin-hadoop2.6/bin/spark-submit --class pref_workload --executor-memory 6G --executor-cores 2 --master "local[2]" target/scala-2.10/pref_workload-assembly-1.0.jar tpch3
# ~/Documents/workspace/spark-1.6.2-bin-hadoop2.6/bin/spark-submit --class pref_workload --executor-memory 6G --executor-cores 2 --master "local[2]" target/scala-2.10/pref_workload-assembly-1.0.jar tpch5
# ~/Documents/workspace/spark-1.6.2-bin-hadoop2.6/bin/spark-submit --class pref_workload --executor-memory 6G --executor-cores 2 --master "local[2]" target/scala-2.10/pref_workload-assembly-1.0.jar tpch6
# ~/Documents/workspace/spark-1.6.2-bin-hadoop2.6/bin/spark-submit --class pref_workload --executor-memory 6G --executor-cores 2 --master "local[2]" target/scala-2.10/pref_workload-assembly-1.0.jar tpch8
# ~/Documents/workspace/spark-1.6.2-bin-hadoop2.6/bin/spark-submit --class pref_workload --executor-memory 6G --executor-cores 2 --master "local[2]" target/scala-2.10/pref_workload-assembly-1.0.jar tpch10
# ~/Documents/workspace/spark-1.6.2-bin-hadoop2.6/bin/spark-submit --class pref_workload --executor-memory 6G --executor-cores 2 --master "local[2]" target/scala-2.10/pref_workload-assembly-1.0.jar tpch12
# ~/Documents/workspace/spark-1.6.2-bin-hadoop2.6/bin/spark-submit --class pref_workload --executor-memory 6G --executor-cores 2 --master "local[2]" target/scala-2.10/pref_workload-assembly-1.0.jar tpch14
# ~/Documents/workspace/spark-1.6.2-bin-hadoop2.6/bin/spark-submit --class pref_workload --executor-memory 6G --executor-cores 2 --master "local[2]" target/scala-2.10/pref_workload-assembly-1.0.jar tpch19

~/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class pref_workload --executor-memory 200G --executor-cores 1 --master spark://istc13.csail.mit.edu:7077 ~/AdaptDB/pref/target/scala-2.10/pref_workload-assembly-1.0.jar tpch3
~/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class pref_workload --executor-memory 200G --executor-cores 1 --master spark://istc13.csail.mit.edu:7077 ~/AdaptDB/pref/target/scala-2.10/pref_workload-assembly-1.0.jar tpch5
~/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class pref_workload --executor-memory 200G --executor-cores 1 --master spark://istc13.csail.mit.edu:7077 ~/AdaptDB/pref/target/scala-2.10/pref_workload-assembly-1.0.jar tpch6
~/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class pref_workload --executor-memory 200G --executor-cores 1 --master spark://istc13.csail.mit.edu:7077 ~/AdaptDB/pref/target/scala-2.10/pref_workload-assembly-1.0.jar tpch8
~/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class pref_workload --executor-memory 200G --executor-cores 1 --master spark://istc13.csail.mit.edu:7077 ~/AdaptDB/pref/target/scala-2.10/pref_workload-assembly-1.0.jar tpch10
~/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class pref_workload --executor-memory 200G --executor-cores 1 --master spark://istc13.csail.mit.edu:7077 ~/AdaptDB/pref/target/scala-2.10/pref_workload-assembly-1.0.jar tpch12
~/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class pref_workload --executor-memory 200G --executor-cores 1 --master spark://istc13.csail.mit.edu:7077 ~/AdaptDB/pref/target/scala-2.10/pref_workload-assembly-1.0.jar tpch14
~/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class pref_workload --executor-memory 200G --executor-cores 1 --master spark://istc13.csail.mit.edu:7077 ~/AdaptDB/pref/target/scala-2.10/pref_workload-assembly-1.0.jar tpch19
