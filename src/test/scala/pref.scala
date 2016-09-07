// Load CMT tables from the datafiles generated
// Start the spark shell using
// ./spark-shell --master spark://istc13.csail.mit.edu:7077 --packages com.databricks:spark-csv_2.11:1.2.0 --executor-memory 200g --driver-memory 10g
// sc is an existing SparkContext.
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// this is used to implicitly convert an RDD to a DataFrame.
import sqlContext.implicits._
import org.apache.spark.sql.SaveMode

val PATH = "hdfs://istc13.csail.mit.edu:9000/user/yilu/tpch1000-spark"
val DEST = "hdfs://istc13.csail.mit.edu:9000/user/yilu/tpch1000-pref-spark"


// Create TPCH tables, lineitem, orders, customer, part and supplier


// Create order table.
sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long,
  o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string,
  o_shippriority int)
USING com.databricks.spark.csv
OPTIONS (path "$PATH/orders", header "false", delimiter "|")""")


// Create lineitem table.
sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long,
	l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double,
	l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string,
	l_shipinstruct string, l_shipmode string)
USING com.databricks.spark.csv
OPTIONS (path "$PATH/lineitem", header "false", delimiter "|")""")


// Create customer table

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string,
c_phone string, c_acctbal double, c_mktsegment string , c_nation string, c_region string)
USING com.databricks.spark.csv
OPTIONS (path "$PATH/customer", header "false", delimiter "|")""")


// Create part table.

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string,
	p_type string, p_size int, p_container string, p_retailprice double)
USING com.databricks.spark.csv
OPTIONS (path "$PATH/part", header "false", delimiter "|")""")


// Create supplier table.

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string,
s_phone string, s_acctbal double, s_nation string, s_region string)
USING com.databricks.spark.csv
OPTIONS (path "$PATH/supplier", header "false", delimiter "|")""")


// query to generate tpchd

//val tpchd = sqlContext.sql(s"""SELECT * FROM lineitem  JOIN orders ON l_orderkey = o_orderkey JOIN customer ON o_custkey = c_custkey  JOIN part ON l_partkey = p_partkey  JOIN supplier ON l_suppkey = s_suppkey""")


val tpchd = sqlContext.sql(s"""SELECT *
FROM lineitem FULL JOIN orders ON l_orderkey = o_orderkey
  FULL JOIN customer ON o_custkey = c_custkey FULL JOIN part ON l_partkey = p_partkey FULL JOIN supplier ON l_suppkey = s_suppkey""")

// repartition

val tpchd_repartitioned = tpchd.repartition(200, $"l_linenumber", $"l_orderkey")
tpchd_repartitioned.registerTempTable("tpchd_repartitioned")

// repartitioned_lineitem

val repartitioned_lineitem = sqlContext.sql(s"""SELECT l_orderkey, l_partkey, l_suppkey,
l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax,
l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate,
l_shipinstruct, l_shipmode
FROM tpchd_repartitioned
WHERE l_orderkey is not null""")

//val repartitioned_lineitem_count = repartitioned_lineitem.count
//val lineitem_count = sqlContext.sql(s"""SELECT COUNT(*) FROM lineitem """).collect

repartitioned_lineitem.registerTempTable("repartitioned_lineitem")
repartitioned_lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/repartitioned_lineitem"), "delimiter" -> "|"))
// repartitioned_orders

val repartitioned_orders = sqlContext.sql(s"""SELECT o_orderkey, o_custkey, o_orderstatus,
o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority
FROM tpchd_repartitioned
WHERE o_orderkey is not null""")

//val repartitioned_orders_count = repartitioned_orders.count
//val orders_count = sqlContext.sql(s"""SELECT COUNT(*) FROM orders """).collect

repartitioned_orders.registerTempTable("repartitioned_orders")
repartitioned_orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/repartitioned_orders"), "delimiter" -> "|"))
// repartitioned_customer

val repartitioned_customer = sqlContext.sql(s"""SELECT c_custkey, c_name, c_address, c_phone, c_acctbal,
  c_mktsegment, c_nation, c_region
FROM tpchd_repartitioned
WHERE c_custkey is not null""")

//val repartitioned_customer_count = repartitioned_customer.count
//val customer_count = sqlContext.sql(s"""SELECT COUNT(*) FROM customer """).collect


repartitioned_customer.registerTempTable("repartitioned_customer")
repartitioned_customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/repartitioned_customer"), "delimiter" -> "|"))

// repartitioned_part

val repartitioned_part = sqlContext.sql(s"""SELECT p_partkey, p_name, p_mfgr, p_brand,
p_type, p_size, p_container, p_retailprice
FROM tpchd_repartitioned
WHERE p_partkey is not null""")

//val repartitioned_part_count = repartitioned_part.count
//val part_count = sqlContext.sql(s"""SELECT COUNT(*) FROM part """).collect

repartitioned_part.registerTempTable("repartitioned_part")
repartitioned_part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/repartitioned_part"), "delimiter" -> "|"))

// repartitioned_supplier

val repartitioned_supplier = sqlContext.sql(s"""SELECT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region
FROM tpchd_repartitioned
WHERE s_suppkey is not null""")

//val repartitioned_supplier_count = repartitioned_supplier.count
//val supplier_count = sqlContext.sql(s"""SELECT COUNT(*) FROM supplier """).collect

repartitioned_supplier.registerTempTable("repartitioned_supplier")
repartitioned_supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/repartitioned_supplier"), "delimiter" -> "|"))


// further testing


sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey int, l_partkey int, l_suppkey int,
	l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double,
	l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string,
	l_shipinstruct string, l_shipmode string)
USING com.databricks.spark.csv
OPTIONS (path "$DEST/repartitioned_lineitem", header "false", delimiter "|")""")


// Create customer table

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey int, c_name string, c_address string,
c_phone string, c_acctbal double, c_mktsegment string , c_nation string, c_region string)
USING com.databricks.spark.csv
OPTIONS (path "$DEST/repartitioned_customer", header "false", delimiter "|")""")


// Create part table.

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey int, p_name string, p_mfgr string, p_brand string,
	p_type string, p_size int, p_container string, p_retailprice double)
USING com.databricks.spark.csv
OPTIONS (path "$DEST/repartitioned_part", header "false", delimiter "|")""")


// Create supplier table.

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey int, s_name string, s_address string,
s_phone string, s_acctbal double, s_nation string, s_region string)
USING com.databricks.spark.csv
OPTIONS (path "$DEST/repartitioned_supplier", header "false", delimiter "|")""")

// Create order table.
sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey int, o_custkey int,
  o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string,
  o_shippriority int)
USING com.databricks.spark.csv
OPTIONS (path "$DEST/repartitioned_orders", header "false", delimiter "|")""")


val lineitem_count = sqlContext.sql(s"""SELECT COUNT(*) FROM lineitem """).collect
val orders_count = sqlContext.sql(s"""SELECT COUNT(*) FROM orders """).collect
val customer_count = sqlContext.sql(s"""SELECT COUNT(*) FROM customer """).collect
val part_count = sqlContext.sql(s"""SELECT COUNT(*) FROM part """).collect
val supplier_count = sqlContext.sql(s"""SELECT COUNT(*) FROM supplier """).collect