#!/usr/bin/python

import sys,os,os.path,time
import tarfile,zipfile
import sqlite3
import libarchive
from common import *

# logging
import logging
logger = logging.getLogger('libarchive')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

###

EXTS_TAR = ('.tar','.tgz','.tbz2','.tar.gz','.tar.bz2')
EXTS_ZIP = ('.zip')
EXTS_ISO9660 = ('.iso','.iso9660')
EXTS_ARCHIVE = (
  '.tar','.tgz','.tbz2',
  '.zip',
  '.iso',
  '.pax', '.cpio', '.xar', '.lha', '.ar', '.cab', '.mtree', '.rar'
)
EXTS_COMPRESS = ('.gz','.bz2','.z','.lz','.xz','.lzma')

numScanned = 0
numAdded = 0
numModified = 0
numRemoved = 0

maindb = None

def joinPaths(a, b):
  return (a + '/' + b).replace('//','/')

def processTarFile(containerKey, path, containerid=None):
  with tarfile.open(path, 'r') as tarf:
    for info in tarf:
      if info.isfile():
        addFileEntry(maindb, joinPaths(containerKey, info.name), info.size, info.mtime, containerid=containerid )

def processZipFile(containerKey, path, containerid=None):
  with zipfile.ZipFile(path, 'r') as zipf:
    for info in zipf.infolist():
      addFileEntry(maindb, joinPaths(containerKey, info.filename), info.file_size, info.date_time, containerid=containerid )

def processArchive(containerKey, path, containerid=None):
  with libarchive.file_reader(path) as archive:
    for entry in archive:
      if entry.isfile:
        addFileEntry(maindb, joinPaths(containerKey, entry.path), entry.size, entry.mtime, containerid=containerid )

def processFile(rootDir, containerKey, filename):
  #print rootDir, containerKey, filename
  #key = u'%s/%s' % (containerKey, filename)
  #path = u'%s/%s' % (rootDir, key)
  key = os.path.join(containerKey, filename)
  path = os.path.join(rootDir, containerKey, filename)
  stat = os.stat(path)
  mtime = min(stat.st_atime, stat.st_mtime, stat.st_ctime)
  size = stat.st_size
  #first,second = os.path.split(key)
  fileid = addFileEntry(maindb, key, size, mtime)
  if type(fileid) == type(0L):
    try:
      fn = filename
      if fn.endswith(EXTS_COMPRESS):
        fn = os.path.splitext(fn)[0]
      if fn.endswith(EXTS_ZIP):
        processZipFile(key, path, containerid=fileid)
      elif fn.endswith(EXTS_ARCHIVE):
        processArchive(key, path, containerid=fileid)
    except KeyboardInterrupt:
      maindb.rollback()
      raise
    except:
      print 'ERROR',sys.exc_info()
      maindb.execute("UPDATE files SET errors=? WHERE id=?", [str(sys.exc_info()[0]), fileid])

def walkDirectory(rootDir, startDir):
  print "Scanning %s (starting at %s)" % (rootDir, startDir)
  numScanned = 0
  totalBytes = 0
  # is it a file?
  if os.path.isfile(startDir):
    key = startDir[len(rootDir)+1:] # TODO
    containerKey, filePath = os.path.split(key)
    return processFile(rootDir, containerKey, filePath)
  # walk the directory tree
  for dirName, subdirList, fileList in os.walk(startDir, topdown=False):
    containerKey = dirName[len(rootDir)+1:] # TODO: slashes matter
    for filePath in fileList:
      processFile(rootDir, containerKey, filePath)
      numScanned += 1
      maindb.commit()
  print "Done. %d files scanned." % (numScanned)

###

def run(args, keywords):
  global maindb
  for arg in args:
    rootDir = findRootDir(arg)
    if not rootDir:
      print "No %s directory found. (Maybe need to init a collection here?)" % (METADIR)
      return False

    metaDir = getMetaDir(rootDir)
    collection = loadCollection(metaDir)
    print "Found collection %s." % (str(collection))
    maindb = openFileDatabase(os.path.join(metaDir, 'files.db'), create=True)
    globaldb = openGlobalDatabase(getGlobalDatabasePath(), create=True)
    url = u"file://%s" % (os.path.abspath(rootDir))
    scanres = ScanResults(collection, url)
    walkDirectory(rootDir, arg)
    scanres.updateFromFilesTable(maindb)
    scanres.addToScansTable(globaldb)
    maindb.close()
    return True

