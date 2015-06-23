#!/usr/bin/python

import commands;
import os;
import re;
import time;
import threading;
import sys;



fReader = open( "/home/mdindex/hadoop-2.6.0/etc/hadoop/slaves", "r" )
nodes = []
for line in fReader:
   nodes.append( line.replace("\n","") )

cmd = sys.argv[1]
threads = [10]
i=0
for node in nodes:
	print "node: ",node
	os.system("ssh %s '%s'" %(node,cmd))
