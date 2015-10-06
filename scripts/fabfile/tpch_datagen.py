from fabric.api import run,cd,parallel,serial
from fabric.contrib.files import exists
counter = 0

@parallel
def tpch_download():
	with cd('/home/mdindex'):
		run('git clone https://github.com/electrum/tpch-dbgen.git')

	with cd('/home/mdindex/tpch-dbgen'):
		run('make')

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

@serial
def create_script_move_denormalized_data():
    global counter
    with cd('/data/mdindex/'):
        script = "#!/bin/bash\n"
        script += "mkdir -p /data/mdindex/tpchd100/\n"
        script += "/home/mdindex/hadoop-2.6.0/bin/hadoop fs -copyToLocal " + \
            "/user/mdindex/lopsc100/part-00%d%d* /data/mdindex/tpchd100/\n" % (counter / 10, counter % 10)
        counter += 1
        script += "/home/mdindex/hadoop-2.6.0/bin/hadoop fs -copyToLocal " + \
            "/user/mdindex/lopsc100/part-00%d%d* /data/mdindex/tpchd100/\n" % (counter / 10, counter % 10)
        run('echo "%s" > move_denormalized_data.sh' % script)
        run('chmod +x move_denormalized_data.sh')

    counter += 1

@parallel
def move_denormalized_data():
    with cd('/data/mdindex/'):
        run('./move_denormalized_data.sh')

