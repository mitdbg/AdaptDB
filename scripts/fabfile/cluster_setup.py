from fabric.api import run,put,cd,parallel,roles

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
        put('hdfs-site.xml', '~/hadoop-2.6.0/etc/hadoop/hdfs-site.xml')
        run('wget http://anilshanbhag.in/confs/core-site.xml')

    with cd('/home/mdindex/spark-1.3.1-bin-hadoop2.6/conf'):
        run('rm -f spark-env.sh')
        run('wget http://anilshanbhag.in/confs/spark-env.sh')

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

@parallel
def copy_ssh_key():
    ssh_key = """ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDgIXPk37ALKlI3aYWeB2wQEVm+LHgJuH9rdZYdTG+YIzbaVamlS+MIVkP+9GJtM5uutyR20Ovk1fJa7Ofpt/KakodQiMxUC0S8AUh+il6t1C+VkUyX5Ejj1HEn2IiuBIHV78PL1Z2vhCRV2J3dRQVSEjVky7B4Uu2qn+DQ2FkXT2WSf8I7+Si0v/XWr/jjCQplNEfSQ2jgXVzKqFFTLIyqQ4Ak9mIcGPHCBwJLFvwcE0spG7RPtpcB6naCTHkYm6ppX5b7cBHU6hM4xU97H7JSswcTV4hmCBw3HMPsFRgYJwSyOzsB1MOpdtyoJXo2mMz1pDWzfNqDUnw0pwLVADVv anil@Anils-MacBook-Pro.local"""
    run('echo "%s" >> ~/.ssh/authorized_keys' % ssh_key)


