#!/usr/bin/python

import os.path

METADIR='.arc'

def getHomeMetaDir():
  return os.path.join(environ['HOME'], METADIR)

###

def findRootDir(path):
  if len(path)<2:
    return None
  if os.path.isdir(path):
    metaDir = os.path.join(path, METADIR)
    if os.path.isdir(metaDir):
      return os.path.dirname(metaDir)
  return findRootDir(os.path.dirname(path))

def getMetaDir(rootDir):
  metaDir = os.path.join(rootDir, METADIR)
  return metaDir

###

if __name__ == '__main__':
  import doctest, metadata
  doctest.testmod(metadata)
