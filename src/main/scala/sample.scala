// Load TPC-H tables from the datafiles generated
// Start the spark shell using
// ./spark-shell --master spark://128.30.77.71:7077 --packages com.databricks:spark-csv_2.11:1.2.0 --driver-memory 4G --executor-memory 200G

// sc is an existing SparkContext.
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// this is used to implicitly convert an RDD to a DataFrame.
import sqlContext.implicits._
import org.apache.spark.sql.SaveMode

val PATH = "hdfs://istc1.csail.mit.edu:9000/user/yilu/tpch1000-spark"
val DEST = "hdfs://istc1.csail.mit.edu:9000/user/yilu/tpch1000-sample"


// Create lineitem table.
val lineitem = sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long,
l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double,
l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string,
l_shipinstruct string, l_shipmode string)
USING com.databricks.spark.csv
OPTIONS (path "$PATH/lineitem", header "false", delimiter "|")""")

//lineitem.registerTempTable("lineitem")


// Create order table.
val orders = sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long,
o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string,
o_shippriority int)
USING com.databricks.spark.csv
OPTIONS (path "$PATH/orders", header "false", delimiter "|")""")


//orders.registerTempTable("orders")



val tpch = sqlContext.sql(s"""SELECT * FROM lineitem FULL JOIN orders ON l_orderkey = o_orderkey""")

tpch.cache();

val tpch20 = tpch.sample(false, 0.2);
tpch20.registerTempTable("tpch20")
val lineitem20 =  sqlContext.sql(s"""SELECT distinct l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity,
l_extendedprice,l_discount, l_tax,l_returnflag, l_linestatus, l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,  l_shipmode from tpch20  """);
lineitem20.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem_20"), "delimiter" -> "|"))
val orders20 =  sqlContext.sql(s"""SELECT distinct o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate,
o_orderpriority,o_clerk, o_shippriority  from tpch20  """);
orders20.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders_20"), "delimiter" -> "|"))


val tpch40 = tpch.sample(false, 0.4);
tpch40.registerTempTable("tpch40")
val lineitem40 =  sqlContext.sql(s"""SELECT distinct l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity,
l_extendedprice,l_discount, l_tax,l_returnflag, l_linestatus, l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,  l_shipmode from tpch40  """);
lineitem40.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem_40"), "delimiter" -> "|"))
val orders40 =  sqlContext.sql(s"""SELECT distinct o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate,
o_orderpriority,o_clerk, o_shippriority  from tpch40  """);
orders40.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders_40"), "delimiter" -> "|"))

val tpch60 = tpch.sample(false, 0.6);
tpch60.registerTempTable("tpch60")
val lineitem60 =  sqlContext.sql(s"""SELECT distinct l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity,
l_extendedprice,l_discount, l_tax,l_returnflag, l_linestatus, l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,  l_shipmode from tpch60  """);
lineitem60.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem_60"), "delimiter" -> "|"))
val orders60 =  sqlContext.sql(s"""SELECT distinct o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate,
o_orderpriority,o_clerk, o_shippriority  from tpch60  """);
orders60.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders_60"), "delimiter" -> "|"))


val tpch80 = tpch.sample(false, 0.8);
tpch80.registerTempTable("tpch80")
val lineitem80 =  sqlContext.sql(s"""SELECT distinct l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity,
l_extendedprice,l_discount, l_tax,l_returnflag, l_linestatus, l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,  l_shipmode from tpch80  """);
lineitem80.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem_80"), "delimiter" -> "|"))
val orders80 =  sqlContext.sql(s"""SELECT distinct o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate,
o_orderpriority,o_clerk, o_shippriority  from tpch80  """);
orders80.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders_80"), "delimiter" -> "|"))

//
