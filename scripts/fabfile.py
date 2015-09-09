from fabric.api import run,put,cd,env,parallel,roles,serial,sudo,local,runs_once
from fabric.contrib.files import exists

env.use_ssh_config = True
env.user = 'mdindex'
env.hosts = ['istc2', 'istc5', 'istc6', 'istc7', 'istc8', 'istc9', 'istc10', 'istc11', 'istc12', 'istc13']

env.roledefs = {
    'master': ['istc2']
}

lineitem_schema = "l_orderkey long, l_partkey int, l_suppkey int, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct string, l_shipmode string, l_comment string"

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
		run('rm -f core-site.xml')
		run('rm -f hdfs-site.xml')
		run('wget http://anilshanbhag.in/confs/hdfs-site.xml')
		run('wget http://anilshanbhag.in/confs/core-site.xml')

	# with cd('/home/mdindex/spark-1.3.1-bin-hadoop2.6/conf'):
	# 	run('rm -f spark-env.sh')
	# 	run('wget http://anilshanbhag.in/confs/spark-env.sh')

	with cd('/data/mdindex'):
		run('rm -f -R dfs')
		run('rm -f -R data')
		run('mkdir dfs')
		run('mkdir data')

	with cd('/home/mdindex'):
		java_home = "export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64"
		run('echo "%s" > .bashrc' % java_home)

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

@parallel
def bulk_sample_gen():
    run('mkdir -p /home/mdindex/logs')

    with cd('~/hadoop-2.6.0/bin/'):
        run('./hadoop fs -mkdir -p /user/mdindex/lineitem1000')
        cmd = './hadoop jar /data/mdindex/jars/mdindex-all.jar perf.benchmark.RunIndexBuilder' + \
            ' --conf /home/mdindex/cartilage.properties' + \
            ' --inputsDir /data/mdindex/lineitem1000/' + \
            ' --samplesDir  /user/mdindex/lineitem1000/samples/' + \
            ' --method 1 ' + \
            ' --numReplicas 1' + \
            ' --samplingRate 0.0002' + \
            (' --schema "%s"' % lineitem_schema) + \
            ' --numFields 16' + \
            ' > ~/logs/sample_stats.log'
        run(cmd)

@roles('master')
def create_robust_tree():
    with cd('~/hadoop-2.6.0/bin/'):
        cmd = './hadoop jar /data/mdindex/jars/mdindex-all.jar perf.benchmark.RunIndexBuilder ' + \
            ' --conf /home/mdindex/cartilage.properties' + \
            ' --inputsDir /data/mdindex/lineitem1000/' + \
            ' --samplesDir  /user/mdindex/lineitem1000/samples/ ' + \
            ' --method 2 ' + \
            ' --numReplicas 1' + \
            ' --numBuckets 8192' + \
            (' --schema "%s"' % lineitem_schema) + \
            ' --numFields 16' + \
            ' > ~/logs/create_tree.log'
        run(cmd)

@parallel
def create_robust_tree_per_replica():
    with cd('~/hadoop-2.6.0/bin/'):
        cmd = './hadoop jar /data/mdindex/jars/mdindex-all.jar perf.benchmark.RunIndexBuilder ' + \
            ' --conf /user/mdindex/cartilage.properties' + \
            ' --inputsDir /data/mdindex/lineitem1000/' + \
            ' --samplesDir  /user/mdindex/lineitem1000/samples/ ' + \
            ' --method 3 ' + \
            ' --numReplicas 3' + \
            ' --numBuckets 8192' + \
            (' --schema "%s"' % lineitem_schema) + \
            ' --numFields 16' + \
            ' > ~/logs/create_replicated_tree.log'
        run(cmd)

@parallel
def write_partitions():
    with cd('~/hadoop-2.6.0/bin/'):
        cmd = './hadoop jar /data/mdindex/jars/mdindex-all.jar perf.benchmark.RunIndexBuilder ' + \
            ' --conf /home/mdindex/cartilage.properties' + \
            ' --inputsDir /data/mdindex/lineitem1000/' + \
            ' --samplesDir  /user/mdindex/lineitem1000/samples/ ' + \
            ' --method 4 ' + \
            ' --numReplicas 1' + \
            ' --numBuckets 8192' + \
            (' --schema "%s"' % lineitem_schema) + \
            ' --numFields 16' + \
            ' > ~/logs/write_partitions.log'
        run(cmd)

@roles('master')
def delete_partitions():
    with cd('~/hadoop-2.6.0/bin/'):
        bp = '/user/mdindex/lineitem1000/partitions'
        paths = ''
        for i in xrange(0,10):
            paths += bp + str(i) + ' '
        cmd = './hadoop fs -rm -R ' + paths
        run(cmd)

@parallel
def check_max_memory():
    with cd('~/hadoop-2.6.0/bin/'):
        cmd = './hadoop jar /data/mdindex/jars/mdindex-all.jar perf.benchmark.RunIndexBuilder ' + \
            ' --conf /home/mdindex/cartilage.properties' + \
            ' --method 5 '
        run(cmd)

