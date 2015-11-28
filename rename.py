#!/usr/bin/python

import os,os.path,sys,uuid
from common import *

def run(args, keywords):
  name = keywords.get('name')
  uuid = keywords.get('uuid')
  if not name and not uuid:
    print "Must specify --name or --uuid"
    sys.exit(2)
  globaldb = openGlobalDatabase(getGlobalDatabasePath())
  if not globaldb:
    print "No host database."
    sys.exit(2)
  if len(args) != 1:
    print "Must specify exactly one collection."
    sys.exit(2)
  cloc = parseCollectionLocation(globaldb, args[0])
  print "Renaming %s" % cloc
  srcdbpath = cloc.getFileDatabasePath()
  if not os.path.exists(srcdbpath):
    print "Cannot find file database @ %s" % srcdbpath
    sys.exit(2)
  if name:
    print "New name = '%s'" % name
    cloc.collection.name = name
  if uuid:
    print "New UUID = '%s'" % uuid
    cloc.collection.uuid = uuid
  destdbpath = cloc.getFileDatabasePath()
  if keywords.get('force'):
    if srcdbpath != destdbpath:
      os.renames(srcdbpath, destdbpath)
    print "Updating '%s'" % cloc.cfgpath
    cloc.collection.write(cloc.cfgpath, True) # TODO: cfgpath in collection?
    sys.exit(0)
  else:
    print "Not updating file unless -f (--force) is set."
    sys.exit(1)
  

