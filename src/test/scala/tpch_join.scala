// Load TPC-H tables from the datafiles generated
// Start the spark shell using
// ./spark-shell --master spark://localhost:7077 --packages com.databricks:spark-csv_2.11:1.2.0 --executor-memory 4g --driver-memory 1g
// sc is an existing SparkContext.
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// this is used to implicitly convert an RDD to a DataFrame.
import sqlContext.implicits._
import org.apache.spark.sql.SaveMode

//val PATH = "hdfs://istc2.csail.mit.edu:9000/user/ylu"

val PATH = "hdfs://localhost:9000/raw"

// Create order table.
sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey int, o_custkey int,
  o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string,
  o_shippriority int, o_comment string)
USING com.databricks.spark.csv
OPTIONS (path "$PATH/orders.tbl", header "false", delimiter "|")""")

// Create lineitem table.
sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey int, l_partkey int, l_suppkey int,
	l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double,
	l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string,
	l_shipinstruct string, l_shipmode string, l_comment string)
USING com.databricks.spark.csv
OPTIONS (path "$PATH/lineitem.tbl", header "false", delimiter "|")""")


// create customer table

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey int, c_name string, c_address string,
  c_nationkey int, c_phone string, c_acctbal double, c_mktsegment string , c_comment string)
USING com.databricks.spark.csv
OPTIONS (path "$PATH/customer.tbl", header "false", delimiter "|")""")

// tpch3

//INFO: Query_cutomer:6:STRING:AUTOMOBILE:LEQ
//INFO: Query_orders:4:DATE:1995-04-14:LT
//INFO: Query_lineitem:10:DATE:1995-04-14:GT


val a = sqlContext.sql(s"""SELECT  COUNT(*)
  FROM lineitem WHERE l_shipdate > "1995-04-14" """)

// 3165950

val b = sqlContext.sql(s"""SELECT  COUNT(*)
  FROM orders WHERE o_orderdate < "1995-04-14" """)

// 746047

val c = sqlContext.sql(s"""SELECT  COUNT(*)
  FROM customer WHERE c_mktsegment <= "AUTOMOBILE" """)

//29752

val d = sqlContext.sql(s"""SELECT  COUNT(*)
  FROM lineitem JOIN orders ON l_orderkey = o_orderkey
	WHERE l_shipdate > "1995-04-14" and  o_orderdate < "1995-04-14" """)

// 150722

val e = sqlContext.sql(s"""SELECT  COUNT(*)
  FROM lineitem JOIN orders ON l_orderkey = o_orderkey JOIN customer ON o_custkey = c_custkey
	WHERE l_shipdate > "1995-04-14" and  o_orderdate < "1995-04-14" and c_mktsegment <= "AUTOMOBILE" """)

// 29569
