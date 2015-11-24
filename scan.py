#!/usr/bin/python

import sys,os,os.path,time
import tarfile,zipfile
import sqlite3
import libarchive
from common import *
from scanner_file import *

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

filesdb = None

def joinPaths(a, b):
  return (a + '/' + b).replace('//','/')

def processTarFile(arcfile, containerid=None):
  with arcfile.getFileHandle() as f:
    with tarfile.open(f, 'r') as tarf:
      for info in tarf:
        if info.isfile():
          sf = ScanFile(joinPaths(arcfile.key, info.name), info.size, info.mtime)
          addFileEntry(filesdb, sf, containerid=containerid)

def processZipFile(arcfile, containerid=None):
  with arcfile.getFileHandle() as f:
    with zipfile.ZipFile(f, 'r') as zipf:
      for info in zipf.infolist():
        sf = ScanFile(joinPaths(arcfile.key, info.filename), info.file_size, info.date_time)
        addFileEntry(filesdb, sf, containerid=containerid)

def processArchive(arcfile, containerid=None):
  with arcfile.getFileHandle() as f:
    with libarchive.fd_reader(f.fileno()) as archive:
      for entry in archive:
        if entry.isfile:
          sf = ScanFile(joinPaths(arcfile.key, entry.path), entry.size, entry.mtime)
          addFileEntry(filesdb, sf, containerid=containerid)

def processScanFile(scanfile):
  fileid = addFileEntry(filesdb, scanfile)
  # file is added/modified, and has file handle
  if type(fileid) == type(0L) and hasattr(scanfile, 'getFileHandle'):
    try:
      fn = scanfile.key
      if fn.endswith(EXTS_COMPRESS):
        fn = os.path.splitext(fn)[0]
      if fn.endswith(EXTS_ZIP):
        processZipFile(scanfile, containerid=fileid)
      elif fn.endswith(EXTS_ARCHIVE):
        processArchive(scanfile, containerid=fileid)
    except KeyboardInterrupt:
      filesdb.rollback()
      raise
    except:
      print 'ERROR',sys.exc_info()
      filesdb.execute("UPDATE files SET errors=? WHERE id=?", [str(sys.exc_info()[1]), fileid])

###

def run(args, keywords):
  global filesdb
  globaldb = openGlobalDatabase(getGlobalDatabasePath(), create=True)
  for arg in args:
    collection = parseCollection(globaldb, arg)
    print "Scanning %s..." % (str(collection))
    filesdb = openFileDatabase(collection.getFileDatabasePath(), create=True)
    scanres = ScanResults(collection, collection.url)
    scanner = FileScanner(collection.url)
    for scanfile in scanner.scan():
      processScanFile(scanfile)
      filesdb.commit()
    print "Scan complete, updating database..."
    scanres.deleteFilesNotSeenSince(filesdb, sessionStartTime)
    scanres.deleteOrphanedFiles(filesdb)
    scanres.updateFromFilesTable(filesdb)
    scanres.addToScansTable(globaldb)
    print "Done."
    filesdb.close()
  globaldb.close()
  return True

