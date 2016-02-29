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
    with cd('/home/mdindex/hadoop-2.6.0/etc/hadoop'):
        run('rm -f core-site.xml')
        run('rm -f hadoop-env.sh')
        run('rm -f hdfs-site.xml')
        put('server/hdfs-site.xml', '~/hadoop-2.6.0/etc/hadoop/hdfs-site.xml')
        put('server/hadoop-env.sh', '~/hadoop-2.6.0/etc/hadoop/hadoop-env.sh')
        put('server/core-site.xml', '~/hadoop-2.6.0/etc/hadoop/core-site.xml')

    with cd('/home/mdindex/spark-1.6.0-bin-hadoop2.6/conf'):
        run('rm -f spark-env.sh')
        run('rm -f spark-defaults.conf')
        put('server/spark-env.sh', '~/spark-1.6.0-bin-hadoop2.6/conf/spark-env.sh')
        put('server/spark-defaults.conf', '~/spark-1.6.0-bin-hadoop2.6/conf/spark-defaults.conf')

    with cd('/data/mdindex'):
        run('rm -f -R dfs')
        run('rm -f -R data')
        run('rm -f -R iotmp')
        run('mkdir dfs')
        run('mkdir data')
        run('mkdir iotmp')

    with cd('/home/mdindex'):
        put('server/.bashrc', '~/')
        put('server/.bash_profile', '~/')


@roles('master')
def copy_scripts():
    run('mkdir -p /home/mdindex/scripts/')
    put('server/config.sh.server', '/home/mdindex/scripts/config.sh')
    put('startSystems.sh', '/home/mdindex/scripts/startSystems.sh')
    put('stopSystems.sh', '/home/mdindex/scripts/stopSystems.sh')
    with cd('/home/mdindex/scripts/'):
        run('chmod +x config.sh')
        run('chmod +x startSystems.sh')
        run('chmod +x stopSystems.sh')

@parallel
def copy_ssh_key():
    # ssh_key = """ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDgIXPk37ALKlI3aYWeB2wQEVm+LHgJuH9rdZYdTG+YIzbaVamlS+MIVkP+9GJtM5uutyR20Ovk1fJa7Ofpt/KakodQiMxUC0S8AUh+il6t1C+VkUyX5Ejj1HEn2IiuBIHV78PL1Z2vhCRV2J3dRQVSEjVky7B4Uu2qn+DQ2FkXT2WSf8I7+Si0v/XWr/jjCQplNEfSQ2jgXVzKqFFTLIyqQ4Ak9mIcGPHCBwJLFvwcE0spG7RPtpcB6naCTHkYm6ppX5b7cBHU6hM4xU97H7JSswcTV4hmCBw3HMPsFRgYJwSyOzsB1MOpdtyoJXo2mMz1pDWzfNqDUnw0pwLVADVv anil@Anils-MacBook-Pro.local"""
    ssh_key = """ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDFPm1eudGSNwlKjkEPMj77A/dhx3eNF3WKZN38SRNPeO/Is0LQ7nH+qakocw8UJintpplu8MNt+DmirfvPbLY+Umm7ADXVCsP5jaAOcqbza98b2zVLer1WrL1HsikFiOlIwIb5rPmtO3v8574GOQ+Plj5mkFx0ppJZGuDrg7ZKzViXGmkJcgUhHV0eJIy3boRq5TWMYtQdQ4D+Wt+EJF7zb4oBhiwikZ0MUf2WEZU6Pjdy4HC4cphdbrxdSTozz6x9CrfoErs0fFlbLyu1acIZV2km28FeR0zw0irkuwa3cy4slkYjEpn2sMf312o2T9vSQav7hQfstQ9tZA5f6zoP mdindex@istc13"""
    run('echo "%s" >> ~/.ssh/authorized_keys' % ssh_key)


