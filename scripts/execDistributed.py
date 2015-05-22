#!/usr/bin/python

import commands;
import os;
import re;
import time;
import threading;
import sys;

#Thread Starting the small instance
class StartC(threading.Thread):

    def __init__(self,node,cmd):
        threading.Thread.__init__(self)
        self.node = node
        self.cmd = cmd

    def run(self):
        os.system("ssh %s '%s'" %(self.node,self.cmd))
        print "Finish on " + self.node
#####


fReader = open( "/home/NODES_FILE", "r" )
nodes = []
for line in fReader:
   nodes.append( line.replace("\n","") )

cmd = sys.argv[1]
threads = [10]
i=0
for node in nodes:
        threads.insert(i, StartC(node,cmd) );
        threads[i].start();
	i = i+1;
i=0
for node in nodes:        
	threads[i].join()
