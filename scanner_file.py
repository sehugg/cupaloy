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

  def __init__(self, url, progress):
    self.rootDir = getDirectoryFromFileURL(url)
    assert os.path.isdir(self.rootDir)
    self.progress = progress
    # TODO: make sure config file exists?
    # TODO: no trailing slash?

  def scan(self):
    startDir = self.rootDir
    for dirName, subdirList, fileList in os.walk(startDir, topdown=True):
      # ignore meta directories (TODO)
      subdirList[:] = filter(lambda x: isIncluded(x), subdirList)
      self.progress.push(os.path.basename(dirName), len(fileList), len(subdirList))
      containerKey = dirName[len(self.rootDir)+1:] # TODO: slashes matter
      if isIncluded(containerKey):
        for filePath in fileList:
          if isIncluded(filePath):
            yield self.processFile(containerKey, filePath)
      self.progress.pop()

  def processFile(self, containerKey, filename):
    key = os.path.join(containerKey, filename)
    path = os.path.join(self.rootDir, containerKey, filename)
    stat = os.stat(path)
    mtime = min(stat.st_atime, stat.st_mtime, stat.st_ctime)
    size = stat.st_size
    self.progress.inc(filename, size)
    return FilesystemFile(key, size, mtime, path)


#for x in FileScanner("file:///home/huggvey/cupaloy/tests").scan():
#  print x
