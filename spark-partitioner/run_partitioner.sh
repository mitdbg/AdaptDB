#!/bin/sh

~/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class spark_partitioner --executor-memory 200G --executor-cores 4 --master spark://istc1.csail.mit.edu:7077 ~/AdaptDB/spark-partitioner/target/scala-2.10/spark-partitioner-assembly-1.0.jar /user/yilu/tpch1000-spark/customer /user/yilu/tpch1000-amoeba/trees/customer_tpch3
~/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class spark_partitioner --executor-memory 200G --executor-cores 4 --master spark://istc1.csail.mit.edu:7077 ~/AdaptDB/spark-partitioner/target/scala-2.10/spark-partitioner-assembly-1.0.jar /user/yilu/tpch1000-spark/customer /user/yilu/tpch1000-amoeba/trees/customer_tpch8
#~/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class spark_partitioner --executor-memory 200G --executor-cores 4 --master spark://istc1.csail.mit.edu:7077 ~/AdaptDB/spark-partitioner/target/scala-2.10/spark-partitioner-assembly-1.0.jar /user/yilu/tpch1000-spark/lineitem /user/yilu/tpch1000-amoeba/trees/lineitem_tpch10
#~/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class spark_partitioner --executor-memory 200G --executor-cores 4 --master spark://istc1.csail.mit.edu:7077 ~/AdaptDB/spark-partitioner/target/scala-2.10/spark-partitioner-assembly-1.0.jar /user/yilu/tpch1000-spark/lineitem /user/yilu/tpch1000-amoeba/trees/lineitem_tpch12
#~/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class spark_partitioner --executor-memory 200G --executor-cores 4 --master spark://istc1.csail.mit.edu:7077 ~/AdaptDB/spark-partitioner/target/scala-2.10/spark-partitioner-assembly-1.0.jar /user/yilu/tpch1000-spark/lineitem /user/yilu/tpch1000-amoeba/trees/lineitem_tpch14
#~/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class spark_partitioner --executor-memory 200G --executor-cores 4 --master spark://istc1.csail.mit.edu:7077 ~/AdaptDB/spark-partitioner/target/scala-2.10/spark-partitioner-assembly-1.0.jar /user/yilu/tpch1000-spark/lineitem /user/yilu/tpch1000-amoeba/trees/lineitem_tpch19
#~/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class spark_partitioner --executor-memory 200G --executor-cores 4 --master spark://istc1.csail.mit.edu:7077 ~/AdaptDB/spark-partitioner/target/scala-2.10/spark-partitioner-assembly-1.0.jar /user/yilu/tpch1000-spark/lineitem /user/yilu/tpch1000-amoeba/trees/lineitem_tpch3
#~/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class spark_partitioner --executor-memory 200G --executor-cores 4 --master spark://istc1.csail.mit.edu:7077 ~/AdaptDB/spark-partitioner/target/scala-2.10/spark-partitioner-assembly-1.0.jar /user/yilu/tpch1000-spark/orders /user/yilu/tpch1000-amoeba/trees/orders
~/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class spark_partitioner --executor-memory 200G --executor-cores 4 --master spark://istc1.csail.mit.edu:7077 ~/AdaptDB/spark-partitioner/target/scala-2.10/spark-partitioner-assembly-1.0.jar /user/yilu/tpch1000-spark/part /user/yilu/tpch1000-amoeba/trees/part
~/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class spark_partitioner --executor-memory 200G --executor-cores 4 --master spark://istc1.csail.mit.edu:7077 ~/AdaptDB/spark-partitioner/target/scala-2.10/spark-partitioner-assembly-1.0.jar /user/yilu/tpch1000-spark/supplier /user/yilu/tpch1000-amoeba/trees/supplier