#!/usr/bin/python

import os.path,urlparse
from common import *
from mount import *

class FilesystemFile(ScanFile):

  def __init__(self, key, size, mtime, path):
    ScanFile.__init__(self, key, size, mtime)
    self.path = path

  def getFileHandle(self):
    return open(self.path, 'rb')

def walkError(err):
  print "Error walking filesystem: %s" % err
  # TODO

class FileScanner:

  def __init__(self, url):
    self.rootDir = getDirectoryFromFileURL(url)
    assert os.path.isdir(self.rootDir)
    self.inodes = set()
    # TODO: make sure config file exists?
    # TODO: no trailing slash?

  def scan(self):
    startDir = self.rootDir
    # TODO: what to do with symlinks? cycles?
    for dirName, subdirList, fileList in os.walk(startDir, topdown=True, onerror=walkError, followlinks=False):
      if verbose:
        print 'dir:',dirName
      containerKey = dirName[len(self.rootDir)+1:] # TODO: slashes matter
      subdirList[:] = filter(lambda dir: isIncluded(os.path.join(containerKey, dir) + os.sep), subdirList)
      if verbose:
        print 'subdirs:',containerKey,subdirList
      if isIncluded(containerKey):
        for filePath in fileList:
          if isIncluded(filePath):
            yield self.processFile(containerKey, filePath)

  def processFile(self, containerKey, filename):
    key = os.path.join(containerKey, filename)
    path = os.path.join(self.rootDir, containerKey, filename)
    if os.path.isfile(path):
      stat = os.stat(path)
      # TODO: how to handle hard links?
      #if stat.st_nlink > 1: # more than 1 hard link?
      #  if stat.st_ino in self.inodes:
      #    return None # already visited this file (hard-linked)
      #  self.inodes.add(stat.st_ino)
      mtime = min(stat.st_atime, stat.st_mtime, stat.st_ctime)
      size = stat.st_size
      return FilesystemFile(key, size, mtime, path)
    else:
      print "Not a regular file: %s" % path
      return None


#for x in FileScanner("file:///home/huggvey/cupaloy/tests").scan():
#  print x
