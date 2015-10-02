from fabric.api import *
from env_setup import *

@parallel
def run_tpch_queries():
    with cd(env.conf['HADOOPBIN']):
        submit_script_path = "/Users/anil/Dev/tools/spark-1.3.1-bin-hadoop2.6/bin/spark-submit"
        #cmd = './hadoop jar $JAR perf.benchmark.TPCHWorkload ' + \
        cmd = submit_script_path + ' --class perf.benchmark.TPCHWorkload --deploy-mode client --master spark://bits:7077 $JAR ' + \
            ' --conf $CONF' + \
            ' --numQueries 3' + \
            ' --schema "$SCHEMA"'  + \
            ' --numFields $NUMFIELDS' + \
            ' --numTuples $NUMTUPLES' + \
            ' --method 1 '
        cmd = fill_cmd(cmd)
        run(cmd)

