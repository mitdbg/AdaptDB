from fabric.api import *
from fabric.contrib.files import exists

counter = 0

@roles('master')
def start_spark():
    run('/home/mdindex/scripts/startSystems.sh')

@roles('master')
def stop_spark():
    run('/home/mdindex/scripts/stopSystems.sh')

@roles('master')
def start_zookeeper():
    run('/home/mdindex/scripts/startZookeeper.sh')

@roles('master')
def stop_zookeeper():
    run('/home/mdindex/scripts/stopZookeeper.sh')

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
        print "Install dtach first !"
        return
    if before:
        cmd = "{}; dtach -n `mktemp -u /tmp/{}.XXXX` {}".format(
            before, sockname, cmd)
    else:
        cmd = "dtach -n `mktemp -u /tmp/{}.XXXX` {}".format(sockname, cmd)
    if use_sudo:
        return sudo(cmd)
    else:
        return run(cmd)

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
    put('/Users/anil/Dev/repos/mdindex/conf/cartilage.properties.server', '/home/mdindex/tpch.properties')
    run('echo "MACHINE_ID = %d" >> /home/mdindex/tpch.properties' % counter)
    counter += 1
