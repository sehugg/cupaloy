#!/usr/bin/python

import os,os.path,sys,uuid
from common import *

def run(args, keywords):
  # TODO: --host?
  globaldb = openGlobalDatabase(getGlobalDatabasePath())
  if not globaldb:
    print "No host database."
    return False
  if len(args) != 1:
    print "Must specify exactly one collection."
    return False
  cloc = parseCollectionLocation(globaldb, args[0])
  print "Retiring %s" % cloc
  destdbpath = cloc.getFileDatabasePath()
  # write changes?
  if keywords.get('force'):
    globaldb.execute("DELETE FROM scans WHERE uuid=? AND url=?", [cloc.collection.uuid, cloc.url])
    if os.path.exists(destdbpath):
      os.remove(destdbpath)
    globaldb.commit()
    print "Removed %s" % destdbpath
    return True
  else:
    print "Not updating file unless -f (--force) is set."
    return False
  

