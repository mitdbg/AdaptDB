from fabric.api import env
from conf_server import conf_server
from conf_local import conf_local

env.use_ssh_config = True

env.conf = None

def setup(mode="server"):
    global conf
    if mode == "server":
        env.conf = conf_server
        env.user = 'mdindex'
        env.hosts = ['istc2.csail.mit.edu', 'istc5.csail.mit.edu', 'istc6.csail.mit.edu', 'istc7.csail.mit.edu', 'istc8.csail.mit.edu', 'istc9.csail.mit.edu', 'istc10.csail.mit.edu', 'istc11.csail.mit.edu', 'istc12.csail.mit.edu', 'istc13.csail.mit.edu']

        env.roledefs = {
            'master': ['istc2.csail.mit.edu']
        }
    else:
        env.conf = conf_local
        env.user = 'anil'
        env.hosts = ['localhost']
        env.roledefs = {
            'master': ['localhost']
        }
