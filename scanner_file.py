#!/usr/bin/python

import os.path,urlparse
from common import *

class FileScanner:

  def __init__(self, url):
    pr = urlparse.urlparse(url)
    assert pr.scheme == 'file'
    self.rootDir = pr.path
    assert os.path.isdir(self.rootDir)

  def scan(self):
    startDir = self.rootDir
    for dirName, subdirList, fileList in os.walk(startDir, topdown=True):
      if METADIR in subdirList:
        subdirList.remove(METADIR) # TODO
      containerKey = dirName[len(self.rootDir)+1:] # TODO: slashes matter
      for filePath in fileList:
        yield self.processFile(containerKey, filePath)

  def processFile(self, containerKey, filename):
    key = os.path.join(containerKey, filename)
    path = os.path.join(self.rootDir, containerKey, filename)
    stat = os.stat(path)
    mtime = min(stat.st_atime, stat.st_mtime, stat.st_ctime)
    size = stat.st_size
    return (key, size, mtime)


for x in FileScanner("file:///tmp").scan():
  print x
