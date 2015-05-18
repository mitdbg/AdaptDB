from fabric.api import *
from fabric.contrib.files import exists

env.use_ssh_config = True
env.user = 'mdindex'
env.hosts = ['istc2', 'istc5', 'istc6', 'istc7', 'istc8', 'istc9', 'istc10', 'istc11', 'istc12', 'istc13']

######################
## Hadoop Setup
######################

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
def conf():
	with cd('/home/mdindex/hadoop-2.6.0/'):
		run('wget ')
	with cd('/home/mdindex/spark'):
		pass
	with cd('/home/mdindex/zookeeper-3.4.6/'):
		pass

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
def tpch_run_dbgen():
	global counter
	with cd('/home/mdindex/tpch-dbgen'):
		for i in xrange(1, 11):
			partition = counter*10 + i
			cmd = './dbgen -T L -s 10000 -C 100 -S %d' % partition
			run_bg(cmd)

	counter += 1
