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
val DEST = "hdfs://localhost:9000/tpch"


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
sqlContext.sql(s"""CREATE TEMPORARY TABLE raworders (o_orderkey int, o_custkey int,
  o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string,
  o_shippriority int, o_comment string)
USING com.databricks.spark.csv
OPTIONS (path "$PATH/orders.tbl", header "false", delimiter "|")""")

val orders = sqlContext.sql(s"""SELECT o_orderkey, o_custkey, o_orderstatus,
o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority
FROM raworders""")

orders.registerTempTable("orders")
orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" ->  (DEST + "/orders"), "delimiter" -> "|"))


// Create raw lineitem table.
sqlContext.sql(s"""CREATE TEMPORARY TABLE rawlineitem (l_orderkey int, l_partkey int, l_suppkey int,
	l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double,
	l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string,
	l_shipinstruct string, l_shipmode string, l_comment string)
USING com.databricks.spark.csv
OPTIONS (path "$PATH/lineitem.tbl", header "false", delimiter "|")""")

val lineitem = sqlContext.sql(s"""SELECT l_orderkey, l_partkey, l_suppkey,
l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax,
l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate,
l_shipinstruct, l_shipmode
FROM rawlineitem""")

lineitem.registerTempTable("orders")
lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem"), "delimiter" -> "|"))


// Create raw customer table

sqlContext.sql(s"""CREATE TEMPORARY TABLE rawcustomer (c_custkey int, c_name string, c_address string,
  c_nationkey int, c_phone string, c_acctbal double, c_mktsegment string , c_comment string)
USING com.databricks.spark.csv
OPTIONS (path "$PATH/customer.tbl", header "false", delimiter "|")""")


val customer = sqlContext.sql(s"""SELECT c_custkey, c_name, c_address, c_phone, c_acctbal,
  c_mktsegment, n_name as c_nation, r_name as c_region
FROM
	rawcustomer JOIN nation ON c_nationkey = n_nationkey
	JOIN region ON n_regionkey = r_regionkey""")

customer.registerTempTable("customer")
customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer"), "delimiter" -> "|"))



// Create raw part table.

sqlContext.sql(s"""CREATE TEMPORARY TABLE rawpart (p_partkey int, p_name string, p_mfgr string, p_brand string,
	p_type string, p_size int, p_container string, p_retailprice double, p_comment string)
USING com.databricks.spark.csv
OPTIONS (path "$PATH/part.tbl", header "false", delimiter "|")""")

val part = sqlContext.sql(s"""SELECT p_partkey, p_name, p_mfgr, p_brand,
p_type, p_size, p_container, p_retailprice
FROM rawpart""")

part.registerTempTable("orders")
part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part"), "delimiter" -> "|"))



// Create raw supplier table.

sqlContext.sql(s"""CREATE TEMPORARY TABLE rawsupplier (s_suppkey int, s_name string, s_address string,
	s_nationkey int, s_phone string, s_acctbal double, s_comment string)
USING com.databricks.spark.csv
OPTIONS (path "$PATH/supplier.tbl", header "false", delimiter "|")""")

val supplier = sqlContext.sql(s"""SELECT s_suppkey, s_name, s_address, s_phone, s_acctbal, n_name AS s_nation, r_name AS s_region
FROM
	rawsupplier JOIN nation ON s_nationkey = n_nationkey
	JOIN region ON n_regionkey = r_regionkey""")

supplier.registerTempTable("supplier")
supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier"), "delimiter" -> "|"))




val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// this is used to implicitly convert an RDD to a DataFrame.
import sqlContext.implicits._
import org.apache.spark.sql.SaveMode


// Create order table.
sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey int, o_custkey int,
  o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string,
  o_shippriority int)
USING com.databricks.spark.csv
OPTIONS (path "hdfs://localhost:9000/tpch/orders/orders.tbl", header "false", delimiter "|")""")


// Create lineitem table.
sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey int, l_partkey int, l_suppkey int,
	l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double,
	l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string,
	l_shipinstruct string, l_shipmode string)
USING com.databricks.spark.csv
OPTIONS (path "hdfs://localhost:9000/tpch/lineitem/lineitem.tbl", header "false", delimiter "|")""")


// Create customer table

sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey int, c_name string, c_address string,
c_phone string, c_acctbal double, c_mktsegment string , c_nation string, c_region string)
USING com.databricks.spark.csv
OPTIONS (path "hdfs://localhost:9000/tpch/customer/customer.tbl", header "false", delimiter "|")""")


// Create part table.

sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey int, p_name string, p_mfgr string, p_brand string,
	p_type string, p_size int, p_container string, p_retailprice double)
USING com.databricks.spark.csv
OPTIONS (path "hdfs://localhost:9000/tpch/part/part.tbl", header "false", delimiter "|")""")


// Create supplier table.

sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey int, s_name string, s_address string,
s_phone string, s_acctbal double, s_nation string, s_region string)
USING com.databricks.spark.csv
OPTIONS (path "hdfs://localhost:9000/tpch/supplier/supplier.tbl", header "false", delimiter "|")""")






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


// tpch5

//INFO: Query_cutomer:7:STRING:AFRICA:LEQ
//INFO: Query_orders:4:DATE:1996-01-01:GEQ;4:DATE:1997-01-01:LT
//INFO: Query_supplier:6:STRING:AFRICA:LEQ

val a = sqlContext.sql(s"""SELECT  COUNT(*)
  FROM customer
	WHERE c_region <= "AFRICA" """)

// 29764

val b = sqlContext.sql(s"""SELECT  COUNT(*)
  FROM orders
	WHERE o_orderdate >= "1996-01-01" and o_orderdate < "1997-01-01"  """)

// 228626

val c = sqlContext.sql(s"""SELECT  COUNT(*)
  FROM customer JOIN orders ON c_custkey = o_custkey
	WHERE c_region <= "AFRICA" and  o_orderdate >= "1996-01-01" and o_orderdate < "1997-01-01"  """)

// 45352

val d = sqlContext.sql(s"""SELECT  COUNT(*)
  FROM supplier
  WHERE s_region <=  "AFRICA" """)

// 1955

val e = sqlContext.sql(s"""SELECT  COUNT(*)
FROM customer JOIN orders ON c_custkey = o_custkey JOIN lineitem ON l_orderkey = o_orderkey
WHERE c_region <= "AFRICA" and  o_orderdate >= "1996-01-01" and o_orderdate < "1997-01-01"  """)

// 182047

val f = sqlContext.sql(s"""SELECT  COUNT(*)
FROM customer JOIN orders ON c_custkey = o_custkey JOIN lineitem ON l_orderkey = o_orderkey JOIN supplier ON l_suppkey = s_suppkey
WHERE c_region <= "AFRICA" and  o_orderdate >= "1996-01-01" and o_orderdate < "1997-01-01" and s_region <=  "AFRICA" """)

// 35307

//tpch6
//INFO: Query_lineitem:10:DATE:1993-01-01:GEQ;10:DATE:1994-01-01:LT;6:DOUBLE:0.0682008692150943:GT;6:DOUBLE:0.08820086921509429:LEQ;4:DOUBLE:25.0:LEQ

val f = sqlContext.sql(s"""SELECT  COUNT(*)
FROM lineitem
WHERE l_shipdate >= "1993-01-01" and l_shipdate < "1994-01-01" and l_discount >= 0.0682008692150943 and  l_discount <= 0.08820086921509429
    and l_quantity <= 25.0 """)


// 83063


//tpch8


//INFO: Query_cutomer:7:STRING:AFRICA:LEQ
//INFO: Query_orders:4:DATE:1995-01-01:GEQ;4:DATE:1996-12-31:LEQ
//INFO: Query_part:4:STRING:STEEL:EQ

val a =  sqlContext.sql(s"""SELECT  COUNT(*)
FROM customer
WHERE c_region <= "AFRICA" """)

// 29764

val b = sqlContext.sql(s"""SELECT  COUNT(*)
FROM part
WHERE p_type =  "STEEL" """)

// 0

val c =  sqlContext.sql(s"""SELECT  COUNT(*)
FROM orders
WHERE o_orderdate >= "1995-01-01" and  o_orderdate <= "1996-12-31" """)

// 457263

val d =  sqlContext.sql(s"""SELECT  COUNT(*)
FROM lineitem JOIN orders ON  l_orderkey = o_orderkey
WHERE o_orderdate >= "1995-01-01" and  o_orderdate <= "1996-12-31" """)

// 1829418

val e =  sqlContext.sql(s"""SELECT  COUNT(*)
FROM lineitem JOIN orders ON  l_orderkey = o_orderkey  JOIN customer ON c_custkey = o_custkey
WHERE o_orderdate >= "1995-01-01" and  o_orderdate <= "1996-12-31" and c_region <= "AFRICA" """)

// 364734

val f = sqlContext.sql(s"""SELECT  COUNT(*)
FROM lineitem JOIN orders ON  l_orderkey = o_orderkey  JOIN customer ON c_custkey = o_custkey JOIN part ON l_partkey = p_partkey
WHERE o_orderdate >= "1995-01-01" and  o_orderdate <= "1996-12-31" and c_region <= "AFRICA" and p_type =  "STEEL" """)

// 0



//tpch10

//INFO: Query_lineitem:8:STRING:R:LEQ;8:STRING:N:GT
//INFO: Query_orders:4:DATE:1993-01-01:GEQ;4:DATE:1993-04-01:LT

val f = sqlContext.sql(s"""SELECT  COUNT(*)
FROM lineitem JOIN orders ON  l_orderkey = o_orderkey  JOIN customer ON c_custkey = o_custkey
WHERE o_orderdate >= "1993-01-01" and  o_orderdate < "1993-04-01" and l_returnflag <= "R" and l_returnflag > "N" """)

// 111918

//tpch12


//INFO: Query_lineitem:14:STRING:SHIP:LEQ;12:DATE:1996-01-01:GEQ;12:DATE:1997-01-01:LT;14:STRING:REG AIR:GT

val f = sqlContext.sql(s"""SELECT  COUNT(*)
FROM lineitem JOIN orders ON  l_orderkey = o_orderkey
WHERE l_shipmode <= "SHIP" and l_shipmode > "REG AIR" and  l_receiptdate >= "1996-01-01" and l_receiptdate < "1997-01-01" """)


//130474

//tpch14
//INFO: Query_lineitem:10:DATE:1993-01-01:GEQ;10:DATE:1993-02-01:LT


val f = sqlContext.sql(s"""SELECT  COUNT(*)
FROM lineitem JOIN part ON  l_partkey = p_partkey
WHERE l_shipdate >= "1993-01-01" and l_shipdate < "1993-02-01" """)

// 76860

//tpch19
//INFO: Query_lineitem:13:STRING:DELIVER IN PERSON:EQ;4:DOUBLE:10.0:GT;4:DOUBLE:20.0:LEQ;14:STRING:AIR:LEQ
//INFO: Query_part:3:STRING:Brand#14:EQ;6:STRING:SM CASE:EQ;5:INT:1:GEQ;5:INT:5:LEQ

val f = sqlContext.sql(s"""SELECT  COUNT(*)
FROM lineitem JOIN part ON  l_partkey = p_partkey
WHERE l_shipinstruct = "DELIVER IN PERSON" and l_quantity > 10 and l_quantity <= 20 and  l_shipmode <= "AIR"
    and p_brand = "Brand#14" and p_container = "SM CASE" and p_size >= 1 and p_size <= 5 """)

// 10