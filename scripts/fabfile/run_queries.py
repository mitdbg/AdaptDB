from fabric.api import *
from env_setup import *

@roles('master')
def run_tpch_queries():
    with cd(env.conf['HADOOPBIN']):
        # submit_script_path = "/home/mdindex/spark-1.3.1-bin-hadoop2.6/bin/spark-submit"
        submit_script_path = "/Users/anil/Dev/tools/spark-1.3.1-bin-hadoop2.6/bin/spark-submit"
        cmd = submit_script_path + ' --class perf.benchmark.TPCHWorkload --deploy-mode client --master spark://128.30.77.88:7077 $JAR ' + \
            ' --conf $CONF' + \
            ' --numQueries 1' + \
            ' --method 1 > ~/logs/tpch_workload.log'
        cmd = fill_cmd(cmd)
        run(cmd)

@roles('master')
def run_cmt_queries():
    with cd(env.conf['HADOOPBIN']):
        submit_script_path = "/home/mdindex/spark-1.3.1-bin-hadoop2.6/bin/spark-submit"
        # submit_script_path = "/Users/anil/Dev/tools/spark-1.3.1-bin-hadoop2.6/bin/spark-submit"
        cmd = submit_script_path + ' --class perf.benchmark.CMTWorkload --deploy-mode client --master spark://128.30.77.88:7077 $JAR ' + \
            ' --conf $CONF' + \
            ' --schema "$SCHEMA"'  + \
            ' --numFields $NUMFIELDS' + \
            ' --numTuples $NUMTUPLES' + \
            ' --method 1 > ~/logs/cmt_workload.log'
        cmd = fill_cmd(cmd)
        run(cmd)

