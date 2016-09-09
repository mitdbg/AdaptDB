#!/usr/bin/python

print './spark-shell --master spark://istc13.csail.mit.edu:7077 --packages com.databricks:spark-csv_2.11:1.2.0 --executor-memory 200g --driver-memory 10g\n'

print 'val sqlContext = new org.apache.spark.sql.SQLContext(sc)\n'

print 'import sqlContext.implicits._'
print 'import org.apache.spark.sql.SaveMode\n'

print 'val PATH = "hdfs://istc13.csail.mit.edu:9000/user/yilu/tpch1000-pref-spark"'
print 'val DEST = "hdfs://istc13.csail.mit.edu:9000/user/yilu/tpch1000-pref"\n'

partitionNum = 200

# select distinct from orders
print '// select distinct from orders\n'

for i in range(partitionNum):
    # create the table
    print 'sqlContext.sql(s"""CREATE TEMPORARY TABLE orders (o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int) USING com.databricks.spark.csv OPTIONS (path "$PATH/orders/part-%05d", header "false", delimiter "|")""")' % i
    # distinct
    print 'val orders = sqlContext.sql(s"""SELECT DISTINCT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority FROM orders""")'
    # save
    print 'orders.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/orders/%d"), "delimiter" -> "|"))\n' % i

# select distinc from lineitem
print '// select distinct from lineitem\n'

for i in range(partitionNum):
    # create the table
    print 'sqlContext.sql(s"""CREATE TEMPORARY TABLE lineitem (l_orderkey long, l_partkey long, l_suppkey long, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string) USING com.databricks.spark.csv OPTIONS (path "$PATH/lineitem/part-%05d", header "false", delimiter "|")""")' % i
    # distinct
    print 'val lineitem = sqlContext.sql(s"""SELECT DISTINCT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode FROM lineitem""")'
    # save
    print 'lineitem.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/lineitem/%d"), "delimiter" -> "|"))\n' % i

# select distinc from customer
print '// select distinct from customer\n'

for i in range(partitionNum):
    # create the table
    print 'sqlContext.sql(s"""CREATE TEMPORARY TABLE customer (c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/customer/part-%05d", header "false", delimiter "|")""")' % i
    # distinct
    print 'val customer = sqlContext.sql(s"""SELECT DISTINCT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_nation, c_region FROM customer""")'
    # save
    print 'customer.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/customer/%d"), "delimiter" -> "|"))\n' % i

# select distinc from part
print '// select distinct from part\n'

for i in range(partitionNum):
    # create the table
    print 'sqlContext.sql(s"""CREATE TEMPORARY TABLE part (p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double) USING com.databricks.spark.csv OPTIONS (path "$PATH/part/part-%05d", header "false", delimiter "|")""")' % i
    # distinct
    print 'val part = sqlContext.sql(s"""SELECT DISTINCT _partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice FROM part""")'
    # save
    print 'part.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/part/%d"), "delimiter" -> "|"))\n' % i

# select distinc from supplier
print '// select distinct from supplier\n'

for i in range(partitionNum):
    # create the table
    print 'sqlContext.sql(s"""CREATE TEMPORARY TABLE supplier (s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string) USING com.databricks.spark.csv OPTIONS (path "$PATH/supplier/part-%05d", header "false", delimiter "|")""")' % i
    # distinct
    print 'val supplier = sqlContext.sql(s"""SELECT DISTINCT s_suppkey, s_name, s_address, s_phone, s_acctbal, s_nation, s_region FROM supplier""")'
    # save
    print 'supplier.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> (DEST + "/supplier/%d"), "delimiter" -> "|"))\n' % i