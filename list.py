#!/usr/bin/python

import sys,os,os.path,string
from common import *

###

def run(args, keywords):
  globaldb = openGlobalDatabase(getGlobalDatabasePath(), create=True)
  for arg in args:
    collection = parseCollection(globaldb, arg)
    filesdb = openFileDatabase(collection.getFileDatabasePath(), create=True)
    for row in filesdb.execute("SELECT path,name,size,modtime FROM files f JOIN folders p ON p.id=f.folder_id"):
      print row
    #TODO
    filesdb.close()
  globaldb.close()

