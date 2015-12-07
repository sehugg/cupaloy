#!/usr/bin/python

import os,os.path,sys,uuid,urlparse
from common import *
from mount import mountInfo

def run(args, keywords):
  name = keywords.get('name')
  if not name:
    print "Must specify --name"
    return False
  if len(args) != 1:
    print "Must specify exactly one existing directory or URL."
    return False
  
  arg = args[0]
  uc = urlparse.urlparse(arg)
  if os.path.isdir(arg):
    # TODO: exists?
    path = arg
    metadir = getMetaDir(path)
    # set or generate UUID?
    uid = keywords.get('uuid')
    if not uid:
      uid = uuid.uuid4()
    volume = mountInfo.getVolumeAt(path)
    url = getURLForVolume(volume, path)
    cl = CollectionLocation(Collection(uid, name), url)
    cl.collection.write(metadir)
  elif uc.scheme and uc.netloc:
    url = arg
    uid = uuid.uuid3(uuid.NAMESPACE_URL, arg)
    cl = CollectionLocation(Collection(uid, name), url)
  else:
    print "Must specify exactly one existing directory or URL."
    return False

  # TODO: exists in host db?  
  globaldb = openGlobalDatabase(getGlobalDatabasePath(), create=True)
  ScanResults(cl).addToScansTable(globaldb)
  globaldb.commit()
  print "Created %s" % (cl)
