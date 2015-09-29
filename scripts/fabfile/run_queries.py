from fabric.api import *
from env_setup import *

@parallel
def run_tpch_queries():
    with cd(env.conf['HADOOPBIN']):
        cmd = './hadoop jar $JAR perf.benchmark.TPCHRunWorkload ' + \
            ' --conf $CONF' + \
            ' --method 1 '
        cmd = fill_cmd(cmd)
        run(cmd)

