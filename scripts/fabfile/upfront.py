from fabric.api import run,put,cd,parallel,roles,serial,local,runs_once
from env_setup import *

counter = 0

def fill_cmd(cmd):
    print env.conf
    for k in env.conf.keys():
        cmd = cmd.replace('$' + k, env.conf[k])
    return cmd

@parallel
def bulk_sample_gen():
    global conf
    run('mkdir -p %slogs' % conf['HOMEDIR'])

    with cd(env.conf['HADOOPBIN']):
        run('./hadoop fs -mkdir -p %s' % env.conf['HDFSDIR'])
        cmd = './hadoop jar $JAR perf.benchmark.RunIndexBuilder' + \
            ' --conf $CONF' + \
            ' --inputsDir $INPUTSDIR' + \
            ' --samplesDir $HDFSDIRsamples/' + \
            ' --method 1 ' + \
            ' --numReplicas 1' + \
            ' --samplingRate $SAMPLINGRATE' + \
            ' --schema "$SCHEMA"'  + \
            ' --numFields $NUMFIELDS' + \
            ' --numTuples $NUMTUPLES' + \
            ' > ~/logs/sample_stats.log'
        cmd = fill_cmd(cmd)
        run(cmd)

@roles('master')
def create_robust_tree():
    global conf
    with cd(env.conf['HADOOPBIN']):
        cmd = './hadoop jar $JAR perf.benchmark.RunIndexBuilder ' + \
            ' --conf $CONF' + \
            ' --inputsDir $INPUTSDIR' + \
            ' --samplesDir $HDFSDIRsamples/' + \
            ' --method 2 ' + \
            ' --numReplicas 1' + \
            ' --numBuckets $NUMBUCKETS' + \
            ' --schema "$SCHEMA"'  + \
            ' --numFields $NUMFIELDS' + \
            ' --numTuples $NUMTUPLES' + \
            ' > ~/logs/create_tree.log'
        cmd = fill_cmd(cmd)
        run(cmd)

@parallel
def create_robust_tree_per_replica():
    with cd(env.conf['HADOOPBIN']):
        cmd = './hadoop jar $JAR perf.benchmark.RunIndexBuilder ' + \
            ' --conf $CONF' + \
            ' --inputsDir $INPUTSDIR' + \
            ' --samplesDir $HDFSDIRsamples/' + \
            ' --method 3 ' + \
            ' --numReplicas 3' + \
            ' --numBuckets $NUMBUCKETS' + \
            ' --schema "$SCHEMA"'  + \
            ' --numFields $NUMFIELDS' + \
            ' --numTuples $NUMTUPLES' + \
            ' > ~/logs/create_replicated_tree.log'
        cmd = fill_cmd(cmd)
        run(cmd)

@parallel
def write_partitions():
    with cd(env.conf['HADOOPBIN']):
        cmd = './hadoop jar $JAR perf.benchmark.RunIndexBuilder ' + \
            ' --conf $CONF' + \
            ' --inputsDir $INPUTSDIR' + \
            ' --samplesDir $HDFSDIRsamples/' + \
            ' --method 4 ' + \
            ' --numReplicas 1' + \
            ' --numBuckets $NUMBUCKETS' + \
            ' --schema "$SCHEMA"'  + \
            ' --numFields $NUMFIELDS' + \
            ' --numTuples $NUMTUPLES' + \
            ' > ~/logs/write_partitions.log'
        cmd = fill_cmd(cmd)
        run(cmd)

@roles('master')
def delete_partitions():
    with cd(env.conf['HADOOPBIN']):
        bp = '%spartitions' % env.conf['HDFSDIR']
        paths = ''
        for i in xrange(0,10):
            paths += bp + str(i) + ' '
        cmd = './hadoop fs -rm -R ' + paths
        cmd = fill_cmd(cmd)
        run(cmd)

@parallel
def check_max_memory():
    with cd(env.conf['HADOOPBIN']):
        cmd = './hadoop jar $JAR perf.benchmark.RunIndexBuilder ' + \
            ' --conf $CONF' + \
            ' --method 5 '
        cmd = fill_cmd(cmd)
        run(cmd)

