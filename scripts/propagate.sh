#!/usr/bin/python

import commands;
import os;
import re;
import time;
import sys;

fReader = open( "/home/mdindex/hadoop-2.6.0/etc/hadoop/slaves", "r" )
nodes = []
for line in fReader:
   nodes.append( line )

file = sys.argv[1]
location = sys.argv[2]
for node in nodes:
   node = node.replace("\n","")
   print "propagating to " + node
   os.system("scp -r %s %s:%s/." %(file,node,location))

print "File Propogation Completed"
