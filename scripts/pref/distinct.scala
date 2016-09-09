./spark-shell --master spark://istc13.csail.mit.edu:7077 --packages com.databricks:spark-csv_2.11:1.2.0 --executor-memory 200g --driver-memory 10g

val sqlContext = new org.apache.spark.sql.SQLContext(sc)

import sqlContext.implicits._
import org.apache.spark.sql.SaveMode

val PATH = "hdfs://istc13.csail.mit.edu:9000/user/yilu/tpch1000-pref-spark"
val DEST = "hdfs://istc13.csail.mit.edu:9000/user/yilu/tpch1000-pref"

// select distinct from orders

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00000", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/0"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00001", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/1"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00002", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/2"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00003", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/3"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00004", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/4"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00005", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/5"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00006", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/6"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00007", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/7"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00008", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/8"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00009", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/9"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00010", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/10"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00011", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/11"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00012", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/12"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00013", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/13"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00014", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/14"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00015", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/15"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00016", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/16"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00017", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/17"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00018", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/18"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00019", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/19"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00020", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/20"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00021", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/21"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00022", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/22"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00023", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/23"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00024", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/24"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00025", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/25"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00026", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/26"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00027", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/27"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00028", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/28"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00029", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/29"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00030", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/30"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00031", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/31"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00032", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/32"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00033", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/33"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00034", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/34"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00035", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/35"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00036", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/36"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00037", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/37"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00038", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/38"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00039", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/39"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00040", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/40"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00041", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/41"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00042", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/42"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00043", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/43"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00044", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/44"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00045", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/45"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00046", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/46"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00047", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/47"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00048", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/48"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00049", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/49"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00050", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/50"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00051", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/51"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00052", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/52"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00053", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/53"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00054", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/54"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00055", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/55"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00056", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/56"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00057", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/57"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00058", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/58"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00059", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/59"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00060", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/60"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00061", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/61"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00062", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/62"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00063", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/63"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00064", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/64"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00065", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/65"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00066", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/66"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00067", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/67"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00068", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/68"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00069", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/69"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00070", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/70"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00071", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/71"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00072", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/72"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00073", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/73"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00074", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/74"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00075", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/75"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00076", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/76"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00077", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/77"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00078", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/78"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00079", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/79"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00080", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/80"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00081", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/81"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00082", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/82"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00083", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/83"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00084", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/84"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00085", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/85"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00086", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/86"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00087", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/87"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00088", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/88"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00089", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/89"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00090", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/90"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00091", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/91"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00092", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/92"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00093", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/93"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00094", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/94"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00095", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/95"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00096", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/96"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00097", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/97"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00098", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/98"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00099", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/99"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00100", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/100"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00101", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/101"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00102", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/102"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00103", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/103"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00104", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/104"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00105", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/105"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00106", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/106"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00107", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/107"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00108", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/108"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00109", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/109"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00110", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/110"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00111", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/111"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00112", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/112"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00113", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/113"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00114", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/114"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00115", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/115"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00116", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/116"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00117", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/117"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00118", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/118"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00119", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/119"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00120", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/120"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00121", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/121"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00122", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/122"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00123", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/123"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00124", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/124"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00125", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/125"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00126", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/126"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00127", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/127"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00128", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/128"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00129", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/129"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00130", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/130"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00131", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/131"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00132", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/132"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00133", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/133"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00134", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/134"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00135", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/135"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00136", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/136"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00137", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/137"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00138", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/138"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00139", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/139"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00140", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/140"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00141", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/141"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00142", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/142"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00143", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/143"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00144", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/144"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00145", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/145"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00146", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/146"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00147", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/147"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00148", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/148"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00149", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/149"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00150", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/150"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00151", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/151"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00152", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/152"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00153", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/153"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00154", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/154"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00155", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/155"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00156", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/156"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00157", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/157"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00158", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/158"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00159", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/159"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00160", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/160"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00161", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/161"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00162", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/162"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00163", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/163"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00164", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/164"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00165", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/165"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00166", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/166"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00167", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/167"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00168", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/168"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00169", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/169"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00170", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/170"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00171", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/171"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00172", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/172"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00173", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/173"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00174", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/174"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00175", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/175"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00176", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/176"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00177", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/177"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00178", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/178"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00179", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/179"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00180", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/180"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00181", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/181"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00182", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/182"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00183", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/183"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00184", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/184"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00185", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/185"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00186", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/186"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00187", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/187"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00188", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/188"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00189", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/189"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00190", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/190"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00191", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/191"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00192", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/192"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00193", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/193"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00194", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/194"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00195", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/195"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00196", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/196"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00197", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/197"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00198", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/198"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-00199", header "false", delimiter "|")""")
val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/199"), "delimiter" -> "|"))

// select distinct from lineitem

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00000", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/0"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00001", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/1"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00002", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/2"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00003", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/3"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00004", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/4"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00005", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/5"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00006", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/6"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00007", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/7"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00008", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/8"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00009", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/9"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00010", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/10"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00011", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/11"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00012", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/12"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00013", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/13"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00014", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/14"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00015", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/15"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00016", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/16"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00017", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/17"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00018", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/18"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00019", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/19"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00020", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/20"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00021", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/21"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00022", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/22"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00023", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/23"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00024", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/24"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00025", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/25"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00026", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/26"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00027", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/27"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00028", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/28"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00029", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/29"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00030", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/30"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00031", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/31"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00032", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/32"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00033", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/33"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00034", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/34"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00035", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/35"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00036", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/36"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00037", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/37"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00038", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/38"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00039", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/39"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00040", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/40"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00041", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/41"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00042", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/42"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00043", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/43"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00044", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/44"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00045", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/45"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00046", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/46"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00047", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/47"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00048", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/48"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00049", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/49"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00050", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/50"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00051", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/51"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00052", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/52"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00053", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/53"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00054", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/54"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00055", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/55"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00056", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/56"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00057", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/57"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00058", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/58"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00059", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/59"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00060", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/60"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00061", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/61"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00062", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/62"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00063", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/63"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00064", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/64"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00065", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/65"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00066", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/66"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00067", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/67"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00068", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/68"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00069", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/69"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00070", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/70"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00071", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/71"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00072", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/72"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00073", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/73"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00074", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/74"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00075", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/75"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00076", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/76"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00077", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/77"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00078", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/78"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00079", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/79"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00080", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/80"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00081", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/81"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00082", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/82"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00083", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/83"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00084", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/84"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00085", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/85"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00086", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/86"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00087", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/87"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00088", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/88"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00089", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/89"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00090", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/90"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00091", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/91"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00092", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/92"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00093", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/93"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00094", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/94"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00095", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/95"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00096", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/96"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00097", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/97"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00098", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/98"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00099", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/99"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00100", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/100"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00101", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/101"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00102", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/102"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00103", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/103"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00104", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/104"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00105", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/105"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00106", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/106"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00107", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/107"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00108", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/108"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00109", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/109"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00110", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/110"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00111", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/111"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00112", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/112"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00113", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/113"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00114", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/114"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00115", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/115"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00116", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/116"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00117", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/117"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00118", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/118"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00119", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/119"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00120", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/120"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00121", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/121"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00122", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/122"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00123", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/123"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00124", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/124"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00125", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/125"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00126", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/126"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00127", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/127"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00128", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/128"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00129", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/129"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00130", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/130"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00131", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/131"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00132", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/132"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00133", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/133"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00134", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/134"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00135", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/135"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00136", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/136"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00137", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/137"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00138", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/138"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00139", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/139"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00140", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/140"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00141", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/141"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00142", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/142"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00143", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/143"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00144", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/144"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00145", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/145"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00146", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/146"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00147", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/147"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00148", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/148"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00149", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/149"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00150", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/150"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00151", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/151"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00152", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/152"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00153", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/153"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00154", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/154"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00155", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/155"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00156", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/156"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00157", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/157"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00158", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/158"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00159", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/159"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00160", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/160"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00161", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/161"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00162", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/162"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00163", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/163"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00164", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/164"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00165", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/165"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00166", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/166"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00167", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/167"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00168", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/168"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00169", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/169"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00170", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/170"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00171", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/171"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00172", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/172"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00173", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/173"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00174", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/174"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00175", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/175"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00176", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/176"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00177", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/177"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00178", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/178"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00179", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/179"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00180", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/180"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00181", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/181"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00182", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/182"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00183", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/183"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00184", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/184"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00185", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/185"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00186", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/186"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00187", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/187"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00188", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/188"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00189", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/189"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00190", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/190"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00191", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/191"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00192", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/192"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00193", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/193"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00194", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/194"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00195", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/195"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00196", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/196"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00197", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/197"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00198", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/198"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-00199", header "false", delimiter "|")""")
val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/199"), "delimiter" -> "|"))

// select distinct from customer

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00000", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/0"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00001", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/1"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00002", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/2"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00003", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/3"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00004", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/4"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00005", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/5"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00006", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/6"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00007", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/7"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00008", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/8"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00009", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/9"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00010", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/10"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00011", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/11"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00012", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/12"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00013", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/13"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00014", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/14"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00015", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/15"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00016", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/16"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00017", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/17"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00018", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/18"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00019", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/19"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00020", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/20"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00021", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/21"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00022", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/22"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00023", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/23"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00024", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/24"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00025", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/25"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00026", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/26"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00027", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/27"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00028", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/28"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00029", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/29"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00030", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/30"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00031", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/31"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00032", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/32"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00033", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/33"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00034", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/34"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00035", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/35"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00036", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/36"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00037", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/37"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00038", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/38"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00039", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/39"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00040", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/40"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00041", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/41"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00042", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/42"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00043", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/43"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00044", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/44"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00045", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/45"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00046", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/46"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00047", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/47"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00048", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/48"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00049", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/49"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00050", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/50"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00051", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/51"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00052", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/52"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00053", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/53"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00054", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/54"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00055", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/55"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00056", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/56"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00057", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/57"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00058", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/58"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00059", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/59"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00060", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/60"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00061", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/61"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00062", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/62"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00063", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/63"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00064", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/64"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00065", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/65"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00066", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/66"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00067", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/67"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00068", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/68"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00069", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/69"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00070", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/70"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00071", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/71"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00072", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/72"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00073", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/73"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00074", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/74"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00075", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/75"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00076", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/76"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00077", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/77"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00078", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/78"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00079", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/79"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00080", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/80"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00081", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/81"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00082", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/82"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00083", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/83"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00084", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/84"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00085", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/85"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00086", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/86"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00087", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/87"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00088", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/88"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00089", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/89"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00090", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/90"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00091", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/91"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00092", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/92"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00093", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/93"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00094", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/94"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00095", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/95"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00096", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/96"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00097", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/97"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00098", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/98"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00099", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/99"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00100", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/100"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00101", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/101"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00102", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/102"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00103", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/103"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00104", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/104"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00105", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/105"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00106", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/106"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00107", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/107"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00108", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/108"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00109", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/109"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00110", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/110"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00111", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/111"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00112", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/112"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00113", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/113"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00114", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/114"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00115", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/115"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00116", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/116"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00117", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/117"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00118", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/118"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00119", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/119"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00120", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/120"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00121", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/121"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00122", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/122"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00123", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/123"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00124", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/124"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00125", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/125"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00126", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/126"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00127", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/127"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00128", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/128"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00129", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/129"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00130", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/130"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00131", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/131"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00132", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/132"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00133", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/133"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00134", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/134"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00135", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/135"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00136", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/136"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00137", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/137"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00138", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/138"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00139", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/139"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00140", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/140"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00141", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/141"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00142", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/142"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00143", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/143"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00144", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/144"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00145", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/145"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00146", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/146"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00147", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/147"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00148", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/148"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00149", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/149"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00150", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/150"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00151", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/151"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00152", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/152"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00153", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/153"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00154", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/154"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00155", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/155"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00156", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/156"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00157", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/157"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00158", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/158"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00159", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/159"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00160", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/160"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00161", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/161"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00162", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/162"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00163", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/163"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00164", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/164"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00165", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/165"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00166", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/166"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00167", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/167"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00168", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/168"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00169", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/169"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00170", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/170"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00171", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/171"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00172", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/172"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00173", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/173"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00174", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/174"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00175", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/175"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00176", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/176"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00177", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/177"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00178", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/178"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00179", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/179"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00180", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/180"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00181", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/181"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00182", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/182"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00183", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/183"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00184", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/184"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00185", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/185"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00186", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/186"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00187", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/187"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00188", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/188"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00189", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/189"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00190", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/190"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00191", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/191"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00192", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/192"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00193", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/193"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00194", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/194"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00195", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/195"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00196", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/196"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00197", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/197"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00198", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/198"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-00199", header "false", delimiter "|")""")
val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/199"), "delimiter" -> "|"))

// select distinct from part

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00000", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/0"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00001", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/1"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00002", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/2"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00003", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/3"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00004", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/4"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00005", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/5"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00006", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/6"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00007", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/7"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00008", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/8"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00009", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/9"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00010", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/10"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00011", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/11"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00012", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/12"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00013", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/13"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00014", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/14"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00015", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/15"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00016", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/16"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00017", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/17"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00018", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/18"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00019", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/19"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00020", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/20"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00021", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/21"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00022", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/22"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00023", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/23"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00024", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/24"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00025", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/25"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00026", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/26"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00027", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/27"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00028", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/28"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00029", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/29"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00030", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/30"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00031", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/31"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00032", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/32"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00033", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/33"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00034", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/34"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00035", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/35"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00036", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/36"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00037", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/37"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00038", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/38"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00039", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/39"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00040", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/40"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00041", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/41"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00042", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/42"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00043", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/43"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00044", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/44"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00045", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/45"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00046", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/46"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00047", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/47"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00048", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/48"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00049", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/49"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00050", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/50"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00051", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/51"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00052", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/52"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00053", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/53"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00054", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/54"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00055", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/55"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00056", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/56"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00057", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/57"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00058", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/58"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00059", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/59"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00060", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/60"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00061", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/61"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00062", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/62"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00063", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/63"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00064", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/64"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00065", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/65"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00066", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/66"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00067", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/67"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00068", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/68"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00069", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/69"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00070", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/70"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00071", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/71"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00072", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/72"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00073", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/73"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00074", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/74"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00075", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/75"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00076", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/76"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00077", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/77"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00078", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/78"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00079", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/79"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00080", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/80"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00081", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/81"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00082", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/82"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00083", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/83"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00084", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/84"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00085", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/85"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00086", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/86"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00087", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/87"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00088", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/88"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00089", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/89"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00090", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/90"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00091", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/91"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00092", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/92"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00093", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/93"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00094", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/94"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00095", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/95"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00096", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/96"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00097", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/97"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00098", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/98"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00099", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/99"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00100", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/100"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00101", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/101"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00102", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/102"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00103", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/103"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00104", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/104"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00105", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/105"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00106", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/106"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00107", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/107"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00108", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/108"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00109", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/109"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00110", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/110"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00111", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/111"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00112", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/112"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00113", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/113"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00114", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/114"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00115", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/115"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00116", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/116"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00117", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/117"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00118", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/118"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00119", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/119"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00120", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/120"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00121", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/121"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00122", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/122"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00123", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/123"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00124", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/124"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00125", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/125"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00126", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/126"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00127", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/127"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00128", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/128"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00129", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/129"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00130", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/130"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00131", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/131"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00132", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/132"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00133", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/133"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00134", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/134"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00135", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/135"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00136", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/136"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00137", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/137"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00138", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/138"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00139", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/139"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00140", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/140"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00141", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/141"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00142", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/142"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00143", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/143"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00144", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/144"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00145", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/145"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00146", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/146"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00147", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/147"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00148", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/148"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00149", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/149"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00150", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/150"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00151", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/151"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00152", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/152"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00153", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/153"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00154", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/154"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00155", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/155"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00156", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/156"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00157", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/157"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00158", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/158"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00159", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/159"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00160", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/160"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00161", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/161"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00162", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/162"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00163", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/163"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00164", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/164"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00165", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/165"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00166", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/166"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00167", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/167"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00168", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/168"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00169", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/169"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00170", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/170"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00171", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/171"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00172", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/172"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00173", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/173"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00174", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/174"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00175", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/175"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00176", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/176"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00177", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/177"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00178", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/178"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00179", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/179"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00180", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/180"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00181", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/181"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00182", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/182"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00183", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/183"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00184", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/184"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00185", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/185"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00186", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/186"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00187", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/187"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00188", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/188"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00189", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/189"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00190", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/190"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00191", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/191"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00192", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/192"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00193", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/193"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00194", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/194"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00195", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/195"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00196", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/196"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00197", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/197"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00198", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/198"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-00199", header "false", delimiter "|")""")
val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/199"), "delimiter" -> "|"))

// select distinct from supplier

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00000", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/0"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00001", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/1"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00002", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/2"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00003", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/3"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00004", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/4"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00005", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/5"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00006", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/6"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00007", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/7"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00008", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/8"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00009", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/9"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00010", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/10"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00011", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/11"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00012", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/12"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00013", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/13"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00014", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/14"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00015", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/15"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00016", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/16"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00017", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/17"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00018", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/18"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00019", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/19"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00020", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/20"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00021", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/21"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00022", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/22"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00023", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/23"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00024", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/24"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00025", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/25"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00026", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/26"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00027", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/27"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00028", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/28"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00029", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/29"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00030", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/30"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00031", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/31"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00032", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/32"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00033", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/33"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00034", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/34"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00035", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/35"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00036", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/36"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00037", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/37"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00038", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/38"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00039", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/39"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00040", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/40"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00041", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/41"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00042", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/42"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00043", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/43"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00044", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/44"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00045", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/45"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00046", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/46"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00047", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/47"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00048", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/48"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00049", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/49"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00050", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/50"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00051", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/51"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00052", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/52"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00053", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/53"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00054", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/54"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00055", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/55"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00056", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/56"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00057", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/57"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00058", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/58"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00059", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/59"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00060", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/60"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00061", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/61"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00062", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/62"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00063", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/63"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00064", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/64"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00065", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/65"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00066", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/66"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00067", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/67"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00068", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/68"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00069", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/69"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00070", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/70"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00071", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/71"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00072", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/72"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00073", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/73"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00074", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/74"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00075", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/75"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00076", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/76"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00077", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/77"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00078", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/78"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00079", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/79"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00080", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/80"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00081", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/81"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00082", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/82"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00083", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/83"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00084", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/84"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00085", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/85"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00086", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/86"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00087", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/87"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00088", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/88"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00089", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/89"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00090", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/90"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00091", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/91"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00092", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/92"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00093", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/93"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00094", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/94"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00095", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/95"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00096", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/96"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00097", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/97"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00098", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/98"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00099", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/99"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00100", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/100"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00101", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/101"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00102", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/102"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00103", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/103"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00104", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/104"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00105", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/105"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00106", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/106"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00107", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/107"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00108", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/108"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00109", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/109"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00110", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/110"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00111", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/111"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00112", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/112"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00113", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/113"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00114", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/114"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00115", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/115"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00116", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/116"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00117", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/117"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00118", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/118"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00119", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/119"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00120", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/120"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00121", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/121"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00122", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/122"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00123", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/123"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00124", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/124"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00125", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/125"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00126", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/126"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00127", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/127"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00128", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/128"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00129", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/129"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00130", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/130"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00131", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/131"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00132", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/132"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00133", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/133"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00134", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/134"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00135", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/135"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00136", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/136"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00137", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/137"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00138", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/138"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00139", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/139"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00140", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/140"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00141", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/141"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00142", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/142"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00143", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/143"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00144", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/144"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00145", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/145"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00146", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/146"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00147", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/147"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00148", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/148"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00149", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/149"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00150", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/150"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00151", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/151"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00152", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/152"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00153", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/153"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00154", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/154"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00155", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/155"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00156", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/156"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00157", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/157"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00158", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/158"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00159", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/159"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00160", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/160"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00161", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/161"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00162", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/162"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00163", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/163"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00164", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/164"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00165", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/165"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00166", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/166"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00167", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/167"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00168", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/168"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00169", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/169"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00170", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/170"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00171", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/171"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00172", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/172"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00173", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/173"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00174", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/174"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00175", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/175"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00176", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/176"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00177", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/177"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00178", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/178"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00179", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/179"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00180", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/180"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00181", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/181"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00182", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/182"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00183", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/183"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00184", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/184"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00185", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/185"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00186", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/186"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00187", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/187"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00188", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/188"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00189", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/189"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00190", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/190"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00191", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/191"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00192", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/192"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00193", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/193"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00194", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/194"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00195", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/195"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00196", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/196"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00197", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/197"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00198", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/198"), "delimiter" -> "|"))

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-00199", header "false", delimiter "|")""")
val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/199"), "delimiter" -> "|"))

