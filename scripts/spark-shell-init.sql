# Used to initialize the tpch table.
# Start the shell using spark-sql --packages com.databricks:spark-csv_2.11:1.3.0

CREATE TABLE tpch (l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct string, l_shipmode string, o_orderstatus string, o_totalprice double, o_orderdate date, o_orderpriority string, o_clerk string, o_shippriority int, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string) USING com.databricks.spark.csv OPTIONS (path "hdfs:///user/mdindex/tpchd1", header "false", delimiter "|");


