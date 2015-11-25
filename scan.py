#!/usr/bin/python

import sys,os,os.path,time
import tarfile,zipfile
import sqlite3
import libarchive
import hashlib
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
force = False
compute_hashes = True

def computeHash(scanfile, hashfn):
  if not compute_hashes:
    return None
  if scanfile.size <= 0:
    return None
  if not hasattr(scanfile, 'getFileHandle'):
    return None
  with scanfile.getFileHandle() as f:
    if f:
      m = hashfn()
      arr = f.read(0x1000)
      while arr:
        m.update(arr)
        arr = f.read(0x1000)
      return m.digest()

def updateHash(db, fileid, scanfile):
  hash_md5 = computeHash(scanfile, hashlib.md5)
  if hash_md5:
    db.execute("UPDATE files SET hash_md5=? WHERE id=?", [buffer(hash_md5), fileid])

def addFileEntry(db, scanfile, containerid=None):
  path = scanfile.key
  mtime = scanfile.mtime
  size = scanfile.size
  # TODO: mtime == 0 or mtime > now
  # TODO: do we really need to convert?
  if type(path) != type(u''):
    try:
      path = path.decode('UTF-8')
    except UnicodeEncodeError:
      print (path, sys.exc_info())
      path = path.decode('cp1252') #TODO?
  folderpath,filename = os.path.split(path)
  mtime = fixTimestamp(mtime)
  folderid = getFolderID(db, folderpath, fileid=containerid)
  cur = db.cursor()
  fileinfo = not force and cur.execute("SELECT id,size,modtime,hash_md5 FROM files WHERE folder_id=? AND name=? AND size=? AND modtime=? AND errors IS NULL", [folderid, filename, size, mtime]).fetchone()
  if fileinfo:
    if not fileinfo[3]: #TODO?
      updateHash(db, fileinfo[0], scanfile)
    cur.execute("UPDATE files SET lastseentime=? WHERE id=?", [sessionStartTime, fileinfo[0]])
    return fileinfo
  else:
    print (folderid, folderpath, filename, size, mtime)
    cur.execute("INSERT OR REPLACE INTO files (folder_id,name,size,modtime,lastseentime,errors) VALUES (?,?,?,?,?,?)", [folderid, filename, size, mtime, sessionStartTime, None])
    fileid = long(cur.lastrowid)
    updateHash(db, fileid, scanfile)
    return fileid

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
  global filesdb, force
  force = 'force' in keywords
  if force:
    print "Forced refresh."
  globaldb = openGlobalDatabase(getGlobalDatabasePath(), create=True)
  for arg in args:
    cloc = parseCollection(globaldb, arg)
    print "Scanning %s" % (str(cloc))
    filesdb = openFileDatabase(cloc.getFileDatabasePath(), create=True)
    scanres = ScanResults(cloc)
    scanner = FileScanner(cloc.url)
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

