#!/usr/bin/python

import os,os.path,sys,uuid
from common import *

def run(args, keywords):
  name = keywords.get('name')
  if not name:
    print "Must specify --name"
    return False
  if len(args) != 1 or not os.path.isdir(args[0]):
    print "Must specify exactly one existing directory."
    return False
  # TODO: exists?
  path = args[0]
  metadir = getMetaDir(path)
  # set or generate UUID?
  uid = keywords.get('uuid')
  if not uid:
    uid = uuid.uuid4()
  cl = CollectionLocation(Collection(uid, name), getFileURL(path))
  cl.collection.write(metadir)
  print "Created %s" % (cl)
