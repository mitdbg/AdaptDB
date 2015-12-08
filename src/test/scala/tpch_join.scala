// Load TPC-H tables from the datafiles generated
// Start the spark shell using
// ./spark-shell --master spark://128.30.77.88:7077 --packages com.databricks:spark-csv_2.11:1.2.0 --executor-memory 10g --driver-memory 4g
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

val a = sqlContext.sql(s"""select count(*) from lineitem where l_returnflag <= "R" and l_returnflag > "N" """);

// 1478870 / 6001215

val b = sqlContext.sql(s"""select count(*) from orders where o_orderdate >= "1993-01-01" and o_orderdate < "1993-04-01" """);

// 55924 / 1500000

val c = sqlContext.sql(s"""SELECT  COUNT(*)
  FROM lineitem JOIN orders ON l_orderkey = o_orderkey
	WHERE l_returnflag <= "R" and l_returnflag > "N" and o_orderdate >= "1993-01-01" and o_orderdate < "1993-04-01" """)

// 111918