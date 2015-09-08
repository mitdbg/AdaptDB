// Load TPC-H tables from the datafiles generated
// Start the spark shell using
// ./spark-shell --master spark://128.30.77.88:7077 --packages com.databricks:spark-csv_2.11:1.2.0

// sc is an existing SparkContext.
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// this is used to implicitly convert an RDD to a DataFrame.
import sqlContext.implicits._

// Create lineitem table.
sqlContext.sql(s"""CREATE TEMPORARY TABLE lineItem (l_orderkey int, l_partkey int, l_suppkey int, l_linenumber int,
	l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string,
	l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string,
	l_comment string)
USING com.databricks.spark.csv
OPTIONS (path "/user/mdindex/lineitem.tbl.1,/user/mdindex/lineitem.tbl.2", header "false", delimiter "|")""")

// Create order table.
sqlContext.sql(s"""CREATE TEMPORARY TABLE order (o_orderkey int, o_custkey int,
  o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string,
  o_shippriority int, o_comment string)
USING com.databricks.spark.csv
OPTIONS (path "/user/mdindex/lineitem.tbl.1", header "false", delimiter "|")""")

// Create partSupplier table.
sqlContext.sql(s"""CREATE TEMPORARY TABLE partSupplier (ps_partkey int, ps_suppkey int,
  ps_availability int, ps_supplycost double, ps_comment string)
USING com.databricks.spark.csv
OPTIONS (path "/user/mdindex/lineitem.tbl.1", header "false", delimiter "|")""")

// Create part table.
sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_name string, p_mfgr string, p_brand string,
	p_type string, p_size int, p_container string, p_retailprice double, p_comment string)
USING com.databricks.spark.csv
OPTIONS (path "/user/mdindex/lineitem.tbl.1", header "false", delimiter "|")""")

// Create supplier table.
sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_name string, s_address string,
	s_phone string, s_acctbal double, s_comment string, s_nation string, s_region string)
USING com.databricks.spark.csv
OPTIONS (path "/user/mdindex/lineitem.tbl.1", header "false", delimiter "|")""")

// Create customer table.
sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_name string, c_address string, c_phone string, c_acctbal double,
  c_mktsegment string , c_comment string , c_nation string , c_region string)
USING com.databricks.spark.csv
OPTIONS (path "/user/mdindex/lineitem.tbl.1", header "false", delimiter "|")""")

// A quick set of ops that can be done on a Dataframe
// df.show()
// df.cache()
// df.printSchema()
// df.select("name").show()
// df.select("name", df("age") + 1).show()
// df.filter(df("name") > 21).show()
// df.groupBy("age").count().show()

val p = sqlContext.sql(s"SELECT COUNT(*) AS T FROM lineItem")


