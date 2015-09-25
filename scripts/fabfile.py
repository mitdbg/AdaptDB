from fabric.api import run,put,cd,env,parallel,roles,serial,sudo,local,runs_once
from fabric.contrib.files import exists
from conf_server import conf_server
from conf_local import conf_local

env.use_ssh_config = True

# Usually run setup:server or setup:local. This are the default settings.
env.user = 'mdindex'
env.hosts = ['istc2', 'istc5', 'istc6', 'istc7', 'istc8', 'istc9', 'istc10', 'istc11', 'istc12', 'istc13']

env.roledefs = {
    'master': ['istc2']
}

conf = None

######################
## Hadoop Setup
######################

## ISTC 2 is the master node
## Make sure that you don't istc2 as the host
## instead set the ip or set the hostname to istc2.csail.mit.edu

@parallel
def download():
	with cd('/home/mdindex'):
		run('wget http://mirror.symnds.com/software/Apache/hadoop/common/hadoop-2.6.0/hadoop-2.6.0.tar.gz')
		run('wget http://d3kbcqa49mib13.cloudfront.net/spark-1.3.1-bin-hadoop2.6.tgz')
		run('wget http://www.motorlogy.com/apache/zookeeper/zookeeper-3.4.6/zookeeper-3.4.6.tar.gz')

@parallel
def untar():
	with cd('/home/mdindex'):
		run('tar -zxvf hadoop-2.6.0.tar.gz')
		run('tar -zxvf spark-1.3.1-bin-hadoop2.6.tgz')
		run('tar -zxvf zookeeper-3.4.6.tar.gz')

@parallel
def configure_systems():
	global hdfssite, coresite, sparkenv
	with cd('/home/mdindex/hadoop-2.6.0/etc/hadoop'):
		#run('rm -f core-site.xml')
		run('rm -f hdfs-site.xml')
        put('hdfs-site.xml', '~/hadoop-2.6.0/etc/hadoop/hdfs-site.xml')
		#run('wget http://anilshanbhag.in/confs/core-site.xml')

	# with cd('/home/mdindex/spark-1.3.1-bin-hadoop2.6/conf'):
	# 	run('rm -f spark-env.sh')
	# 	run('wget http://anilshanbhag.in/confs/spark-env.sh')

   # with cd('/data/mdindex'):
		#run('rm -f -R dfs')
		#run('rm -f -R data')
		#run('mkdir dfs')
		#run('mkdir data')

   # with cd('/home/mdindex'):
		#java_home = "export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64"
		#run('echo "%s" > .bashrc' % java_home)

@roles('master')
def copy_scripts():
    run('mkdir -p /home/mdindex/scripts/')
    put('/Users/anil/Dev/repos/mdindex/scripts/config.sh.server', '/home/mdindex/scripts/config.sh')
    put('/Users/anil/Dev/repos/mdindex/scripts/startSystems.sh', '/home/mdindex/scripts/startSystems.sh')
    put('/Users/anil/Dev/repos/mdindex/scripts/stopSystems.sh', '/home/mdindex/scripts/stopSystems.sh')
    put('/Users/anil/Dev/repos/mdindex/scripts/startZookeeper.sh', '/home/mdindex/scripts/startZookeeper.sh')
    put('/Users/anil/Dev/repos/mdindex/scripts/stopZookeeper.sh', '/home/mdindex/scripts/stopZookeeper.sh')
    with cd('/home/mdindex/scripts/'):
        run('chmod +x config.sh')
        run('chmod +x startSystems.sh')
        run('chmod +x stopSystems.sh')
        run('chmod +x startZookeeper.sh')
        run('chmod +x stopZookeeper.sh')

@roles('master')
def start_zookeeper():
    run('/home/mdindex/scripts/startZookeeper.sh')

@roles('master')
def stop_zookeeper():
    run('/home/mdindex/scripts/stopZookeeper.sh')

@roles('master')
def start_spark():
    run('/home/mdindex/scripts/startSystems.sh')

@roles('master')
def stop_spark():
    run('/home/mdindex/scripts/stopSystems.sh')

@parallel
def kill_all():
    run('pkill java')

@roles('master')
def start_all():
    start_spark()
    start_zookeeper()

@roles('master')
def stop_all():
    stop_zookeeper()
    stop_spark()

@parallel
def copy_ssh_key():
    ssh_key = """ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDgIXPk37ALKlI3aYWeB2wQEVm+LHgJuH9rdZYdTG+YIzbaVamlS+MIVkP+9GJtM5uutyR20Ovk1fJa7Ofpt/KakodQiMxUC0S8AUh+il6t1C+VkUyX5Ejj1HEn2IiuBIHV78PL1Z2vhCRV2J3dRQVSEjVky7B4Uu2qn+DQ2FkXT2WSf8I7+Si0v/XWr/jjCQplNEfSQ2jgXVzKqFFTLIyqQ4Ak9mIcGPHCBwJLFvwcE0spG7RPtpcB6naCTHkYm6ppX5b7cBHU6hM4xU97H7JSswcTV4hmCBw3HMPsFRgYJwSyOzsB1MOpdtyoJXo2mMz1pDWzfNqDUnw0pwLVADVv anil@Anils-MacBook-Pro.local"""
    run('echo "%s" >> ~/.ssh/authorized_keys' % ssh_key)

#########################
## TPCH DataGen
#########################

@parallel
def tpch_download():
	with cd('/home/mdindex'):
		run('git clone https://github.com/electrum/tpch-dbgen.git')

	with cd('/home/mdindex/tpch-dbgen'):
		run('make')

def run_bg(cmd, before=None, sockname="dtach", use_sudo=False):
    """Run a command in the background using dtach

    :param cmd: The command to run
    :param output_file: The file to send all of the output to.
    :param before: The command to run before the dtach. E.g. exporting
                   environment variable
    :param sockname: The socket name to use for the temp file
    :param use_sudo: Whether or not to use sudo
    """
    if not exists("/usr/bin/dtach"):
        sudo("apt-get install dtach")
    if before:
        cmd = "{}; dtach -n `mktemp -u /tmp/{}.XXXX` {}".format(
            before, sockname, cmd)
    else:
        cmd = "dtach -n `mktemp -u /tmp/{}.XXXX` {}".format(sockname, cmd)
    if use_sudo:
        return sudo(cmd)
    else:
        return run(cmd)

counter = 0

# Note parallel doesn't work with global counter
@serial
def tpch_script_gen():
	global counter
	try:
		with cd('/data/mdindex/tpch-dbgen'):
			script = "#!/bin/sh\n"
			for i in xrange(1, 11):
				partition = counter*10 + i
				cmd = './dbgen -T L -s 10000 -C 100 -S %d &\n' % partition
				script += cmd

			run('echo "%s" > data_gen.sh' % script)
	except:
		pass

	counter += 1

def setup(mode="server"):
    global conf
    if mode == "server":
        conf = conf_server
        env.user = 'mdindex'
        env.hosts = ['istc2', 'istc5', 'istc6', 'istc7', 'istc8', 'istc9', 'istc10', 'istc11', 'istc12', 'istc13']
        env.roledefs = {
            'master': ['istc2']
        }
    else:
        conf = conf_local
        env.user = 'anil'
        env.hosts = ['localhost']
        env.roledefs = {
            'master': ['localhost']
        }

@serial
def tpch_script_gen_100():
    global counter
    try:
        if not exists('/data/mdindex/tpch100'):
            run('mkdir /data/mdindex/tpch100')

        with cd('/data/mdindex/tpch-dbgen'):
            try:
                # delete any old tables before creating new ones
                run('rm *.tbl.%d' % (counter + 1))
            except:
                pass
            script = "#!/bin/bash\n"
            script += "cd /data/mdindex/tpch-dbgen/\n"
            cmd = './dbgen -s 100 -C 10 -S %d\n' % (counter + 1)
            script += cmd

            run('echo "%s" > data_gen.sh' % script)
            run('chmod +x data_gen.sh')
    except:
        pass

    counter += 1

@parallel
def tpch_gen_100():
    run('nohup /data/mdindex/tpch-dbgen/data_gen.sh >> /tmp/xxx 2>&1 < /dev/null &', pty=False)

@serial
def create_script_move_data():
    global counter
    with cd('/data/mdindex/'):
        script = "#!/bin/bash\n"
        script += "/home/mdindex/hadoop-2.6.0/bin/hadoop fs -copyFromLocal " + \
            "/data/mdindex/tpch-dbgen/*.tbl.%d /user/mdindex/tpch100/" % (counter + 1)
        run('echo "%s" > move_data.sh' % script)
        run('chmod +x move_data.sh')

    counter += 1

@parallel
def move_data():
    run('nohup /data/mdindex/move_data.sh >> /tmp/xxx 2>&1 < /dev/null &', pty=False)

@runs_once
def build_jar():
    local('cd /Users/anil/Dev/repos/mdindex/; gradle shadowJar')

@parallel
def update_jar():
    if not exists('/data/mdindex/jars'):
        run('mkdir /data/mdindex/jars')
    put('/Users/anil/Dev/repos/mdindex/build/libs/mdindex-all.jar', '/data/mdindex/jars/')

@serial
def update_config():
    global counter
    put('/Users/anil/Dev/repos/mdindex/conf/cartilage.properties.server', '/home/mdindex/cartilage.properties')
    run('echo "MACHINE_ID = %d" >> /home/mdindex/cartilage.properties' % counter)
    counter += 1

def fill_cmd(cmd):
    global conf
    print conf
    for k in conf.keys():
        cmd = cmd.replace('$' + k, conf[k])
    return cmd

@parallel
def bulk_sample_gen():
    global conf
    run('mkdir -p %slogs' % conf['HOMEDIR'])

    with cd(conf['HADOOPBIN']):
        run('./hadoop fs -mkdir -p %s' % conf['HDFSDIR'])
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
def create_robust_tree(mode='server'):
    global conf
    with cd(conf['HADOOPBIN']):
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
def create_robust_tree_per_replica(mode='server'):
    global conf
    with cd('~/hadoop-2.6.0/bin/'):
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
def write_partitions(mode='server'):
    global conf
    with cd(conf['HADOOPBIN']):
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
def delete_partitions(mode='server'):
    global conf
    with cd(conf['HADOOPBIN']):
        bp = '%spartitions' % conf['HDFSDIR']
        paths = ''
        for i in xrange(0,10):
            paths += bp + str(i) + ' '
        cmd = './hadoop fs -rm -R ' + paths
        cmd = fill_cmd(cmd)
        run(cmd)

@parallel
def check_max_memory():
    with cd('~/hadoop-2.6.0/bin/'):
        cmd = './hadoop jar /data/mdindex/jars/mdindex-all.jar perf.benchmark.RunIndexBuilder ' + \
            ' --conf /home/mdindex/cartilage.properties' + \
            ' --method 5 '
        run(cmd)

@roles('master')
def test_dtach():
    run('nohup sleep 100 >> /tmp/xxx 2>&1 < /dev/null &', pty=False)

@roles('master')
def sum_bucket_counts():
    with cd('~/hadoop-2.6.0/bin/'):
        cmd = './hadoop jar /data/mdindex/jars/mdindex-all.jar perf.benchmark.BucketCounterManager ' + \
            ' --conf /home/mdindex/cartilage.properties' + \
            ' --method 0 --start 0 --end 31'
        run(cmd)

@roles('master')
def delete_bucket_counts():
    with cd('~/hadoop-2.6.0/bin/'):
        cmd = './hadoop jar /data/mdindex/jars/mdindex-all.jar perf.benchmark.BucketCounterManager ' + \
            ' --conf /home/mdindex/cartilage.properties' + \
            ' --method 1'
        run(cmd)

@serial
def create_lineitem_100():
    global counter
    with cd('/data/mdindex/'):
        run('mkdir lineitem100')
        run('cp tpch-dbgen/lineitem.tbl.%d lineitem100/' % (counter + 1))
        counter += 1

