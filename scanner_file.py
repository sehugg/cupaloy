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
  

class FileScanner:

  def __init__(self, url):
    pr = urlparse.urlparse(url)
    assert pr.scheme == 'file'
    # if file://netloc/, prepend root of mount to path
    # TODO?
    if pr.netloc and len(pr.netloc):
      self.rootDir = os.path.normpath(os.path.join(mountLocationForUUID(pr.netloc), pr.path[1:]))
    else:
      self.rootDir = pr.path
    assert os.path.isdir(self.rootDir)
    # TODO: no trailing slash?

  def scan(self):
    startDir = self.rootDir
    for dirName, subdirList, fileList in os.walk(startDir, topdown=True):
      # ignore meta directories (TODO)
      if METADIR in subdirList:
        subdirList.remove(METADIR)
      containerKey = dirName[len(self.rootDir)+1:] # TODO: slashes matter
      for filePath in fileList:
        yield self.processFile(containerKey, filePath)

  def processFile(self, containerKey, filename):
    key = os.path.join(containerKey, filename)
    path = os.path.join(self.rootDir, containerKey, filename)
    stat = os.stat(path)
    mtime = min(stat.st_atime, stat.st_mtime, stat.st_ctime)
    size = stat.st_size
    return FilesystemFile(key, size, mtime, path)


#for x in FileScanner("file:///home/huggvey/cupaloy/tests").scan():
#  print x
