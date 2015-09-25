conf_server = {
    'JAR' : '/data/mdindex/jars/mdindex-all.jar',
    'CONF' : '/home/mdindex/cartilage.properties',
    'INPUTSDIR' : '/data/mdindex/lineitem1000/',
    'HDFSDIR' : '/user/mdindex/lineitem1000/',
    'HOMEDIR' : '/home/mdindex/',
    'HADOOPBIN' : '~/hadoop-2.6.0/bin/',
    'SAMPLINGRATE' : '0.0002',
    'NUMBUCKETS' : '8192',
    'NUMTUPLES' : '6000000000',
    'NUMFIELDS' : '16',
    'SCHEMA': 'l_orderkey long, l_partkey int, l_suppkey int, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct string, l_shipmode string, l_comment string',
}
