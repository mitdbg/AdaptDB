from fabric.api import run,put,cd,parallel,roles,serial,local,runs_once
from env_setup import *

counter = 0

@parallel
def bulk_sample_gen():
    global conf
    run('mkdir -p %slogs' % env.conf['HOMEDIR'])

    with cd(env.conf['HADOOPBIN']):
        run('./hadoop fs -mkdir -p %s' % env.conf['HDFSDIR'])
        cmd = './hadoop jar $JAR perf.benchmark.RunIndexBuilder' + \
            ' --conf $CONF' + \
            ' --tableName $TABLENAME' + \
            ' --inputsDir $INPUTSDIR' + \
            ' --method 1 ' + \
            ' --numReplicas 1' + \
            ' --samplingRate $SAMPLINGRATE' + \
            ' --delimiter "$DELIMITER"' + \
            ' --schema "$SCHEMA"'  + \
            ' --numFields $NUMFIELDS' + \
            ' --numTuples $NUMTUPLES' + \
            ' > ~/logs/sample_stats.log'
        cmd = fill_cmd(cmd)
        run(cmd)

@roles('master')
def create_robust_tree():
    global conf
    print env.roledefs
    with cd(env.conf['HADOOPBIN']):
        cmd = './hadoop jar $JAR perf.benchmark.RunIndexBuilder ' + \
            ' --conf $CONF' + \
            ' --tableName $TABLENAME' + \
            ' --inputsDir $INPUTSDIR' + \
            ' --samplesDir $HDFSDIRsamples/' + \
            ' --method 2 ' + \
            ' --numReplicas 1' + \
            ' --numBuckets $NUMBUCKETS' + \
            ' --delimiter "$DELIMITER"' + \
            ' --schema "$SCHEMA"'  + \
            ' --numFields $NUMFIELDS' + \
            ' --numTuples $NUMTUPLES' + \
            ' > ~/logs/create_tree.log'
        cmd = fill_cmd(cmd)
        run(cmd)

@roles('master')
def write_out_sample():
    global conf
    with cd(env.conf['HADOOPBIN']):
        cmd = './hadoop jar $JAR perf.benchmark.RunIndexBuilder ' + \
            ' --conf $CONF' + \
            ' --tableName $TABLENAME' + \
            ' --inputsDir $INPUTSDIR' + \
            ' --method 6 ' + \
            ' --numReplicas 1' + \
            ' --numBuckets $NUMBUCKETS' + \
            ' --delimiter "$DELIMITER"' + \
            ' --schema "$SCHEMA"'  + \
            ' --numFields $NUMFIELDS' + \
            ' --numTuples $NUMTUPLES' + \
            ' > ~/logs/write_sample.log'
        cmd = fill_cmd(cmd)
        run(cmd)

@parallel
def write_partitions():
    with cd(env.conf['HADOOPBIN']):
        cmd = './hadoop jar $JAR perf.benchmark.RunIndexBuilder ' + \
            ' --conf $CONF' + \
            ' --tableName $TABLENAME' + \
            ' --inputsDir $INPUTSDIR' + \
            ' --method 4 ' + \
            ' --numReplicas 1' + \
            ' --numBuckets $NUMBUCKETS' + \
            ' --delimiter "$DELIMITER"' + \
            ' --schema "$SCHEMA"'  + \
            ' --numFields $NUMFIELDS' + \
            ' --numTuples $NUMTUPLES' + \
            ' > ~/logs/write_partitions.log'
        cmd = fill_cmd(cmd)
        run(cmd)

@roles('master')
def delete_partitions():
    with cd(env.conf['HADOOPBIN']):
        cmd = './hadoop fs -rm -R $HDFSDIR'
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

