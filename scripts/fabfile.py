from fabric.api import *
from fabric.contrib.files import exists
from conf import *

env.use_ssh_config = True
env.user = 'mdindex'
env.hosts = ['istc2', 'istc5', 'istc6', 'istc7', 'istc8', 'istc9', 'istc10', 'istc11', 'istc12', 'istc13']
# env.hosts = ['istc1', 'istc4']
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

# Note parallel doesn't work with global counter
@serial
def tpch_script_gen():
	global counter
	try:
		with cd('/home/mdindex/tpch-dbgen'):
			script = "#!/bin/sh\n"
			for i in xrange(1, 11):
				partition = counter*10 + i
				cmd = './dbgen -T L -s 10000 -C 100 -S %d &\n' % partition
				script += cmd

			run('echo "%s" > data_gen.sh' % script)
	except:
		pass

	counter += 1

@parallel
def enable_ssh():
	# Setting istc2 as the master node
	with cd('/home/mdindex/.ssh/'):
		ssh_key = 'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDgr8w9ukEIXuAciuGbPGAr2nHqTNjupWKZQdx2pMTdQ8l3p1SuzJq0ZzNI7alCc+smJUHUOJf8+Iy4X67l7fkmQIswDGq/5g6POai/urOy18drKoEII7pG1NLekJQLzTqKe74IdQmBwO6LWlFCZmQQg0Jaixm/Dhbh/XYY3laVZvO6hMeP2lhE24WEhG6iPsS0H2MM95nit5Xz/NkMqYg6bqDAwC0aaQhT7EhMIt6nLL4Th+owqCdMLOQ2PL2yCzrfPiA5JyFMbKk04mlLNT6NkwQvkL0UqQx5WtEEx4LRY6XIhEVdk8tn5FkvVdqsfhOpuYU9FPIXnU99FpPWrd15 mdindex@istc2'
		run('echo "%s" >> authorized_keys' % ssh_key)


#####################################
## Sample Generator
#####################################
@parallel
def download_sampler():
	with cd('/home/mdindex/hadoop-2.6.0/etc/hadoop/'):
		run('mv hadoop-env.sh hadoop-env.sh.backup')
		run('wget http://anilshanbhag.in/hadoop-env.sh')

	# with cd('/data/mdindex/tpch-dbgen/'):
		# run('mv sampler.jar.3 sampler.jar')
		# run('wget http://anilshanbhag.in/sampler2.jar')

@serial
def bulk_sample_gen():
	global counter
	with cd('~/hadoop-2.6.0/bin/'):
		script = "#!/bin/sh\n"
		for i in xrange(1, 11):
			partition = counter*10 + i
			cmd = './hadoop jar /data/mdindex/tpch-dbgen/sampler2.jar core.index.Sampler /data/mdindex/tpch-dbgen/lineitem/lineitem.tbl.%d &\n' % partition
			script += cmd

		run('echo "%s" > sample_gen.sh' % script)
		run('chmod +x sample_gen.sh')
	counter += 1