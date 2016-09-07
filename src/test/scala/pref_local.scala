// Load CMT tables from the datafiles generated
// Start the spark shell using
// ./spark-shell --master spark://26-2-88.dynamic.csail.mit.edu:7077 --packages com.databricks:spark-csv_2.11:1.2.0 --executor-memory 4g --driver-memory 1g
// sc is an existing SparkContext.
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// this is used to implicitly convert an RDD to a DataFrame.
import sqlContext.implicits._
import org.apache.spark.sql.SaveMode

val PATH = "hdfs://localhost:9000/user/ylu/raw_tpch1"
val DEST = "hdfs://localhost:9000/user/ylu/tpch1"


// Create nation table.
sqlContext.sql(s"""CREATE TEMPORARY TABLE nation (n_nationkey int, n_name string, n_regionkey int,
	n_comment string)
USING com.databricks.spark.csv
OPTIONS (path "$PATH/nation.tbl", header "false", delimiter "|")""")

// Create region table.
sqlContext.sql(s"""CREATE TEMPORARY TABLE region (r_regionkey int, r_name string, r_comment string)
USING com.databricks.spark.csv
OPTIONS (path "$PATH/region.tbl", header "false", delimiter "|")""")



// Create raw order table.
sqlContext.sql(s"""CREATE TEMPORARY TABLE raw_orders (o_orderkey int, o_custkey int,
  o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string,
  o_shippriority int, o_comment string)
USING com.databricks.spark.csv
OPTIONS (path "$PATH/orders.tbl", header "false", delimiter "|")""")

val orders = sqlContext.sql(s"""SELECT o_orderkey, o_custkey, o_orderstatus,
o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority
FROM raw_orders""")

orders.registerTempTable("orders")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" ->  (DEST + "/orders"), "delimiter" -> "|"))


// Create raw lineitem table.
sqlContext.sql(s"""CREATE TEMPORARY TABLE raw_lineitem (l_orderkey int, l_partkey int, l_suppkey int,
	l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double,
	l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string,
	l_shipinstruct string, l_shipmode string, l_comment string)
USING com.databricks.spark.csv
OPTIONS (path "$PATH/lineitem.tbl", header "false", delimiter "|")""")


val lineitem = sqlContext.sql(s"""SELECT l_orderkey, l_partkey, l_suppkey,
l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax,
l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate,
l_shipinstruct, l_shipmode
FROM raw_lineitem""")

lineitem.registerTempTable("lineitem")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem"), "delimiter" -> "|"))


// Create raw customer table

sqlContext.sql(s"""CREATE TEMPORARY TABLE raw_customer (c_custkey int, c_name string, c_address string,
  c_nationkey int, c_phone string, c_acctbal double, c_mktsegment string , c_comment string)
USING com.databricks.spark.csv
OPTIONS (path "$PATH/customer.tbl", header "false", delimiter "|")""")


val customer = sqlContext.sql(s"""SELECT c_custkey, c_name, c_address, c_phone, c_acctbal,
  c_mktsegment, n_name as c_nation, r_name as c_region
FROM
	raw_customer JOIN nation ON c_nationkey = n_nationkey
	JOIN region ON n_regionkey = r_regionkey""")

customer.registerTempTable("customer")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer"), "delimiter" -> "|"))



// Create raw part table.

sqlContext.sql(s"""CREATE TEMPORARY TABLE raw_part (p_partkey int, p_name string, p_mfgr string, p_brand string,
	p_type string, p_size int, p_container string, p_retailprice double, p_comment string)
USING com.databricks.spark.csv
OPTIONS (path "$PATH/part.tbl", header "false", delimiter "|")""")

val part = sqlContext.sql(s"""SELECT p_partkey, p_name, p_mfgr, p_brand,
p_type, p_size, p_container, p_retailprice
FROM raw_part""")

part.registerTempTable("part")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part"), "delimiter" -> "|"))



// Create raw supplier table.

sqlContext.sql(s"""CREATE TEMPORARY TABLE raw_supplier (s_suppkey int, s_name string, s_address string,
	s_nationkey int, s_phone string, s_acctbal double, s_comment string)
USING com.databricks.spark.csv
OPTIONS (path "$PATH/supplier.tbl", header "false", delimiter "|")""")

val supplier = sqlContext.sql(s"""SELECT s_suppkey, s_name, s_address, s_phone, s_acctbal, n_name AS s_nation, r_name AS s_region
FROM
	raw_supplier JOIN nation ON s_nationkey = n_nationkey
	JOIN region ON n_regionkey = r_regionkey""")

supplier.registerTempTable("supplier")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier"), "delimiter" -> "|"))


// Create TPCH tables, lineitem, orders, customer, part and supplier


// Create order table.
sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey int, o_custkey int,
  o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string,
  o_shippriority int)
USING com.databricks.spark.csv
OPTIONS (path "$DEST/orders", header "false", delimiter "|")""")


// Create lineitem table.
sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey int, l_partkey int, l_suppkey int,
	l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double,
	l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string,
	l_shipinstruct string, l_shipmode string)
USING com.databricks.spark.csv
OPTIONS (path "$DEST/lineitem", header "false", delimiter "|")""")


// Create customer table

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey int, c_name string, c_address string,
c_phone string, c_acctbal double, c_mktsegment string , c_nation string, c_region string)
USING com.databricks.spark.csv
OPTIONS (path "$DEST/customer", header "false", delimiter "|")""")


// Create part table.

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey int, p_name string, p_mfgr string, p_brand string,
	p_type string, p_size int, p_container string, p_retailprice double)
USING com.databricks.spark.csv
OPTIONS (path "$DEST/part", header "false", delimiter "|")""")


// Create supplier table.

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey int, s_name string, s_address string,
s_phone string, s_acctbal double, s_nation string, s_region string)
USING com.databricks.spark.csv
OPTIONS (path "$DEST/supplier", header "false", delimiter "|")""")


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

val repartitioned_lineitem_count = repartitioned_lineitem.count
//val lineitem_count = sqlContext.sql(s"""SELECT COUNT(*) FROM lineitem """).collect

repartitioned_lineitem.registerTempTable("repartitioned_lineitem")
repartitioned_lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/repartitioned_lineitem"), "delimiter" -> "|"))
// repartitioned_orders

val repartitioned_orders = sqlContext.sql(s"""SELECT o_orderkey, o_custkey, o_orderstatus,
o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority
FROM tpchd_repartitioned
WHERE o_orderkey is not null""")

val repartitioned_orders_count = repartitioned_orders.count
//val orders_count = sqlContext.sql(s"""SELECT COUNT(*) FROM orders """).collect

repartitioned_orders.registerTempTable("repartitioned_orders")
repartitioned_orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/repartitioned_orders"), "delimiter" -> "|"))
// repartitioned_customer

val repartitioned_customer = sqlContext.sql(s"""SELECT c_custkey, c_name, c_address, c_phone, c_acctbal,
  c_mktsegment, c_nation, c_region
FROM tpchd_repartitioned
WHERE c_custkey is not null""")

val repartitioned_customer_count = repartitioned_customer.count
//val customer_count = sqlContext.sql(s"""SELECT COUNT(*) FROM customer """).collect


repartitioned_customer.registerTempTable("repartitioned_customer")
repartitioned_customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/repartitioned_customer"), "delimiter" -> "|"))

// repartitioned_part

val repartitioned_part = sqlContext.sql(s"""SELECT p_partkey, p_name, p_mfgr, p_brand,
p_type, p_size, p_container, p_retailprice
FROM tpchd_repartitioned
WHERE p_partkey is not null""")

val repartitioned_part_count = repartitioned_part.count
//val part_count = sqlContext.sql(s"""SELECT COUNT(*) FROM part """).collect

repartitioned_part.registerTempTable("repartitioned_part")
repartitioned_part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/repartitioned_part"), "delimiter" -> "|"))

// repartitioned_supplier

val repartitioned_supplier = sqlContext.sql(s"""SELECT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region
FROM tpchd_repartitioned
WHERE s_suppkey is not null""")

val repartitioned_supplier_count = repartitioned_supplier.count
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