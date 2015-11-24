#!/usr/bin/python

import sys,os,os.path,time
from common import *

###

def run(args, keywords):
  globaldb = openGlobalDatabase(getGlobalDatabasePath(), create=True)
  for arg in args:
    collection = parseCollection(globaldb, arg)
    filesdb = openFileDatabase(collection.getFileDatabasePath(), create=True)
    #TODO
    filesdb.close()
  globaldb.close()

