from fabric.api import run,cd,parallel,serial
from fabric.contrib.files import exists
counter = 0

@parallel
def cmt_init():
    if not exists('/data/mdindex/yilu'):
        run('mkdir /data/mdindex/yilu')
    with cd('/data/mdindex/yilu'):
        run('git clone https://github.com/luyi0619/cmt-dbgen.git')

    with cd('/data/mdindex/yilu/cmt-dbgen'):
        run('javac src/dataGenerator.java')

@serial
def cmt_script_gen_100000000():
    global counter
    try:
        with cd('/data/mdindex/yilu/cmt-dbgen'):
            try:
                # delete any old tables before creating new ones
                run('rm -rf *txt*')
            except:
                pass
            
            script = "#!/bin/bash\n"
            script += "cd /data/mdindex/yilu/cmt-dbgen/src\n"
            cmd = 'java dataGenerator ../dist 10 %d 100000000\n' % counter
            script += cmd

            run('echo "%s" > data_gen.sh' % script)
            run('chmod +x data_gen.sh')
    except:
        pass

    counter += 1


@parallel
def cmt_gen():
    run('nohup /data/mdindex/yilu/cmt-dbgen/data_gen.sh  > /dev/null 2>&1 < /dev/null &', pty=False)


@parallel
def cmt_move_data_100000000():
    if exists('/data/mdindex/yilu'):
        run('rm -rf /data/mdindex/yilu/cmt100000000')
    run('mkdir -p /data/mdindex/yilu/cmt100000000/mh')
    run('mkdir -p /data/mdindex/yilu/cmt100000000/mhl')
    run('mkdir -p /data/mdindex/yilu/cmt100000000/sf')
    run('mv /data/mdindex/yilu/cmt-dbgen/src/mapmatch_history.txt.* /data/mdindex/yilu/cmt100000000/mh')
    run('mv /data/mdindex/yilu/cmt-dbgen/src/mapmatch_history_latest.txt.* /data/mdindex/yilu/cmt100000000/mhl')
    run('mv /data/mdindex/yilu/cmt-dbgen/src/sf_datasets.txt.* /data/mdindex/yilu/cmt100000000/sf')
    run('rm -rf /data/mdindex/yilu/cmt-dbgen/src/*txt*')
    
@parallel
def cmt_script_upload_data():
    with cd('/data/mdindex/yilu'):
        script = "#!/bin/bash\n"
        script += "/home/mdindex/hadoop-2.6.0/bin/hadoop fs -mkdir /user/yilu/cmt100000000\n"
        script += "/home/mdindex/hadoop-2.6.0/bin/hadoop fs -mkdir /user/yilu/cmt100000000/mh\n"
        script += "/home/mdindex/hadoop-2.6.0/bin/hadoop fs -mkdir /user/yilu/cmt100000000/mhl\n"
        script += "/home/mdindex/hadoop-2.6.0/bin/hadoop fs -mkdir /user/yilu/cmt100000000/sf\n"
        script += "/home/mdindex/hadoop-2.6.0/bin/hadoop fs -copyFromLocal " + \
                "/data/mdindex/yilu/cmt100000000/mh/* /user/yilu/cmt100000000/mh\n" 
        script += "/home/mdindex/hadoop-2.6.0/bin/hadoop fs -copyFromLocal " + \
                "/data/mdindex/yilu/cmt100000000/mhl/* /user/yilu/cmt100000000/mhl\n"
        script += "/home/mdindex/hadoop-2.6.0/bin/hadoop fs -copyFromLocal " + \
                "/data/mdindex/yilu/cmt100000000/sf/* /user/yilu/cmt100000000/sf\n"
        run('echo "%s" > upload_data.sh' % script)
        run('chmod +x upload_data.sh')

@parallel
def cmt_upload_data():
    run('nohup /data/mdindex/yilu/upload_data.sh  > /dev/null 2>&1 < /dev/null &', pty=False)
  
