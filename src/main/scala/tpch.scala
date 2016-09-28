// Load TPC-H tables from the datafiles generated
// Start the spark shell using
// ./spark-shell --master spark://128.30.77.71:7077 --packages com.databricks:spark-csv_2.11:1.2.0 --driver-memory 4G --executor-memory 200G

// sc is an existing SparkContext.
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// this is used to implicitly convert an RDD to a DataFrame.
import sqlContext.implicits._
import org.apache.spark.sql.SaveMode

val PATH = "hdfs://istc1.csail.mit.edu:9000/user/yilu/tpch1000-raw"
val DEST = "hdfs://istc1.csail.mit.edu:9000/user/yilu/tpch1000-spark"


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
sqlContext.sql(s"""CREATE TEMPORARY TABLE raworders (o_orderkey long, o_custkey long,
  o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string,
  o_shippriority int, o_comment string)
USING com.databricks.spark.csv
OPTIONS (path "$PATH/orders.tbl.1,$PATH/orders.tbl.2,$PATH/orders.tbl.3,$PATH/orders.tbl.4,$PATH/orders.tbl.5,$PATH/orders.tbl.6,$PATH/orders.tbl.7,$PATH/orders.tbl.8,$PATH/orders.tbl.9", header "false", delimiter "|")""")

val orders = sqlContext.sql(s"""SELECT o_orderkey, o_custkey, o_orderstatus,
o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority
FROM raworders""")

orders.registerTempTable("orders")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" ->  (DEST + "/orders"), "delimiter" -> "|"))


// Create raw lineitem table.
sqlContext.sql(s"""CREATE TEMPORARY TABLE rawlineitem (l_orderkey long, l_partkey long, l_suppkey long,
	l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double,
	l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string,
	l_shipinstruct string, l_shipmode string, l_comment string)
USING com.databricks.spark.csv
OPTIONS (path "$PATH/lineitem.tbl.1,$PATH/lineitem.tbl.2,$PATH/lineitem.tbl.3,$PATH/lineitem.tbl.4,$PATH/lineitem.tbl.5,$PATH/lineitem.tbl.6,$PATH/lineitem.tbl.7,$PATH/lineitem.tbl.8,$PATH/lineitem.tbl.9", header "false", delimiter "|")""")


val lineitem = sqlContext.sql(s"""SELECT l_orderkey, l_partkey, l_suppkey,
l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax,
l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate,
l_shipinstruct, l_shipmode
FROM rawlineitem""")

lineitem.registerTempTable("orders")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem"), "delimiter" -> "|"))


// Create raw customer table

sqlContext.sql(s"""CREATE TEMPORARY TABLE rawcustomer (c_custkey long, c_name string, c_address string,
  c_nationkey int, c_phone string, c_acctbal double, c_mktsegment string , c_comment string)
USING com.databricks.spark.csv
OPTIONS (path "$PATH/customer.tbl.1,$PATH/customer.tbl.2,$PATH/customer.tbl.3,$PATH/customer.tbl.4,$PATH/customer.tbl.5,$PATH/customer.tbl.6,$PATH/customer.tbl.7,$PATH/customer.tbl.8,$PATH/customer.tbl.9", header "false", delimiter "|")""")


val customer = sqlContext.sql(s"""SELECT c_custkey, c_name, c_address, c_phone, c_acctbal,
  c_mktsegment, n_name as c_nation, r_name as c_region
FROM
	rawcustomer JOIN nation ON c_nationkey = n_nationkey
	JOIN region ON n_regionkey = r_regionkey""")

customer.registerTempTable("customer")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer"), "delimiter" -> "|"))



// Create raw part table.

sqlContext.sql(s"""CREATE TEMPORARY TABLE rawpart (p_partkey long, p_name string, p_mfgr string, p_brand string,
	p_type string, p_size int, p_container string, p_retailprice double, p_comment string)
USING com.databricks.spark.csv
OPTIONS (path "$PATH/part.tbl.1,$PATH/part.tbl.2,$PATH/part.tbl.3,$PATH/part.tbl.4,$PATH/part.tbl.5,$PATH/part.tbl.6,$PATH/part.tbl.7,$PATH/part.tbl.8,$PATH/part.tbl.9", header "false", delimiter "|")""")

val part = sqlContext.sql(s"""SELECT p_partkey, p_name, p_mfgr, p_brand,
p_type, p_size, p_container, p_retailprice
FROM rawpart""")

part.registerTempTable("orders")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part"), "delimiter" -> "|"))



// Create raw supplier table.

sqlContext.sql(s"""CREATE TEMPORARY TABLE rawsupplier (s_suppkey long, s_name string, s_address string,
	s_nationkey int, s_phone string, s_acctbal double, s_comment string)
USING com.databricks.spark.csv
OPTIONS (path "$PATH/supplier.tbl.1,$PATH/supplier.tbl.2,$PATH/supplier.tbl.3,$PATH/supplier.tbl.4,$PATH/supplier.tbl.5,$PATH/supplier.tbl.6,$PATH/supplier.tbl.7,$PATH/supplier.tbl.8,$PATH/supplier.tbl.9", header "false", delimiter "|")""")

val supplier = sqlContext.sql(s"""SELECT s_suppkey, s_name, s_address, s_phone, s_acctbal, n_name AS s_nation, r_name AS s_region
FROM
	rawsupplier JOIN nation ON s_nationkey = n_nationkey
	JOIN region ON n_regionkey = r_regionkey""")

supplier.registerTempTable("supplier")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier"), "delimiter" -> "|"))

//