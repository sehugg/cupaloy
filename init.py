#!/usr/bin/python

import os,os.path,sys,uuid
from common import *

def run(args, keywords):
  name = keywords.get('name')
  if not name:
    print "Must specify --name"
    sys.exit(2)
  if len(args) != 1 or not os.path.isdir(args[0]):
    print "Must specify exactly one existing directory."
    sys.exit(2)
  # TODO: exists?
  path = args[0]
  metadir = getMetaDir(path)
  uid = uuid.uuid4()
  coll = Collection(uid, name, getFileURL(path))
  coll.write(metadir)
  print "Collection '%s' created (UUID %s)." % (name, uid)
