#!/usr/bin/python

import os,os.path,sys,uuid
from common import *

def run(args, keywords):
  name = keywords.get('name')
  uuid = keywords.get('uuid')
  if not name and not uuid:
    print "Must specify --name or --uuid"
    return False
  # TODO: --host?
  globaldb = openGlobalDatabase(getGlobalDatabasePath())
  if not globaldb:
    print "No host database."
    return False
  if len(args) != 1:
    print "Must specify exactly one collection."
    return False
  cloc = parseCollectionLocation(globaldb, args[0])
  print "Renaming %s" % cloc
  srcdbpath = cloc.getFileDatabasePath()
  if not os.path.exists(srcdbpath):
    print "Cannot find file database @ %s" % srcdbpath
    return False
  if name:
    print "New name = '%s'" % name
    cloc.collection.name = name
  if uuid:
    print "New UUID = '%s'" % uuid
    cloc.collection.uuid = uuid
  destdbpath = cloc.getFileDatabasePath()
  # write changes?
  if keywords.get('force'):
    if srcdbpath != destdbpath:
      os.renames(srcdbpath, destdbpath)
    # TODO: what if not directory-based?
    cfgfn = os.path.join(getDirectoryFromFileURL(cloc.url), METADIR)
    assert os.path.exists(cfgfn)
    cloc.collection.write(cfgfn, True)
    # TODO: update global db?
    return True
  else:
    print "Not updating file unless -f (--force) is set."
    return False
  

