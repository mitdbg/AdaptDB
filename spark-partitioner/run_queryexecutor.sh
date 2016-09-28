#!/bin/sh

~/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class spark_queryexecutor --executor-memory 200G --executor-cores 1 --master spark://istc1.csail.mit.edu:7077 ~/AdaptDB/spark-partitioner/target/scala-2.10/spark-partitioner-assembly-1.0.jar hdfs://istc1.csail.mit.edu:9000/user/yilu/tpch1000-amoeba/trees/lineitem_tpch3 hdfs://istc1.csail.mit.edu:9000/user/yilu/tpch1000-amoeba/trees/orders hdfs://istc1.csail.mit.edu:9000/user/yilu/tpch1000-amoeba/trees/customer_tpch3 hdfs://istc1.csail.mit.edu:9000/user/yilu/tpch1000-amoeba/trees/part hdfs://istc1.csail.mit.edu:9000/user/yilu/tpch1000-amoeba/trees/supplier tpch3


~/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class spark_queryexecutor --executor-memory 200G --executor-cores 1 --master spark://istc1.csail.mit.edu:7077 ~/AdaptDB/spark-partitioner/target/scala-2.10/spark-partitioner-assembly-1.0.jar hdfs://istc1.csail.mit.edu:9000/user/yilu/tpch1000-amoeba/trees/lineitem_tpch3 hdfs://istc1.csail.mit.edu:9000/user/yilu/tpch1000-amoeba/trees/orders hdfs://istc1.csail.mit.edu:9000/user/yilu/tpch1000-amoeba/trees/customer_tpch3 hdfs://istc1.csail.mit.edu:9000/user/yilu/tpch1000-amoeba/trees/part hdfs://istc1.csail.mit.edu:9000/user/yilu/tpch1000-amoeba/trees/supplier tpch5

~/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class spark_queryexecutor --executor-memory 200G --executor-cores 1 --master spark://istc1.csail.mit.edu:7077 ~/AdaptDB/spark-partitioner/target/scala-2.10/spark-partitioner-assembly-1.0.jar hdfs://istc1.csail.mit.edu:9000/user/yilu/tpch1000-amoeba/trees/lineitem_tpch3 hdfs://istc1.csail.mit.edu:9000/user/yilu/tpch1000-amoeba/trees/orders hdfs://istc1.csail.mit.edu:9000/user/yilu/tpch1000-amoeba/trees/customer_tpch8 hdfs://istc1.csail.mit.edu:9000/user/yilu/tpch1000-amoeba/trees/part hdfs://istc1.csail.mit.edu:9000/user/yilu/tpch1000-amoeba/trees/supplier tpch8

~/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class spark_queryexecutor --executor-memory 200G --executor-cores 1 --master spark://istc1.csail.mit.edu:7077 ~/AdaptDB/spark-partitioner/target/scala-2.10/spark-partitioner-assembly-1.0.jar hdfs://istc1.csail.mit.edu:9000/user/yilu/tpch1000-amoeba/trees/lineitem_tpch10 hdfs://istc1.csail.mit.edu:9000/user/yilu/tpch1000-amoeba/trees/orders hdfs://istc1.csail.mit.edu:9000/user/yilu/tpch1000-amoeba/trees/customer_tpch3 hdfs://istc1.csail.mit.edu:9000/user/yilu/tpch1000-amoeba/trees/part hdfs://istc1.csail.mit.edu:9000/user/yilu/tpch1000-amoeba/trees/supplier tpch10


~/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class spark_queryexecutor --executor-memory 200G --executor-cores 1 --master spark://istc1.csail.mit.edu:7077 ~/AdaptDB/spark-partitioner/target/scala-2.10/spark-partitioner-assembly-1.0.jar hdfs://istc1.csail.mit.edu:9000/user/yilu/tpch1000-amoeba/trees/lineitem_tpch12 hdfs://istc1.csail.mit.edu:9000/user/yilu/tpch1000-amoeba/trees/orders hdfs://istc1.csail.mit.edu:9000/user/yilu/tpch1000-amoeba/trees/customer_tpch3 hdfs://istc1.csail.mit.edu:9000/user/yilu/tpch1000-amoeba/trees/part hdfs://istc1.csail.mit.edu:9000/user/yilu/tpch1000-amoeba/trees/supplier tpch12

~/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class spark_queryexecutor --executor-memory 200G --executor-cores 6 --master spark://istc1.csail.mit.edu:7077 ~/AdaptDB/spark-partitioner/target/scala-2.10/spark-partitioner-assembly-1.0.jar hdfs://istc1.csail.mit.edu:9000/user/yilu/tpch1000-amoeba/trees/lineitem_tpch14 hdfs://istc1.csail.mit.edu:9000/user/yilu/tpch1000-amoeba/trees/orders hdfs://istc1.csail.mit.edu:9000/user/yilu/tpch1000-amoeba/trees/customer_tpch3 hdfs://istc1.csail.mit.edu:9000/user/yilu/tpch1000-amoeba/trees/part hdfs://istc1.csail.mit.edu:9000/user/yilu/tpch1000-amoeba/trees/supplier tpch14

~/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class spark_queryexecutor --executor-memory 200G --executor-cores 1 --master spark://istc1.csail.mit.edu:7077 ~/AdaptDB/spark-partitioner/target/scala-2.10/spark-partitioner-assembly-1.0.jar hdfs://istc1.csail.mit.edu:9000/user/yilu/tpch1000-amoeba/trees/lineitem_tpch19 hdfs://istc1.csail.mit.edu:9000/user/yilu/tpch1000-amoeba/trees/orders hdfs://istc1.csail.mit.edu:9000/user/yilu/tpch1000-amoeba/trees/customer_tpch3 hdfs://istc1.csail.mit.edu:9000/user/yilu/tpch1000-amoeba/trees/part hdfs://istc1.csail.mit.edu:9000/user/yilu/tpch1000-amoeba/trees/supplier tpch19