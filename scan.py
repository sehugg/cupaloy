#!/usr/bin/python

import sys,os,os.path,time,traceback
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
EXTS_COMPRESS = ('.gz','.bz2','.z','.lz','.xz','.lzma','.uu')

filesdb = None
force = False
compute_hashes = True

def computeHashOfFile(f, hashfn):
  """
  >>> import binascii
  >>> binascii.hexlify(computeHashOfFile(open('tests/root/emptyfile.txt','r'), hashlib.md5))
  'd41d8cd98f00b204e9800998ecf8427e'
  >>> binascii.hexlify(computeHashOfFile(open('tests/archives/root/rabin.tgz.bz2','r'), hashlib.md5))
  '704ca9f2a35f52960f4f94926991241b'
  """
  m = hashfn()
  arr = f.read(0x1000)
  while arr:
    m.update(arr)
    arr = f.read(0x1000)
  return m.digest()

def computeHash(scanfile, hashfn):
  if not compute_hashes:
    return None
  if scanfile.size <= 0:
    return None
  if not hasattr(scanfile, 'getFileHandle'):
    return None
  with scanfile.getFileHandle() as f:
    if f:
      return computeHashOfFile(f, hashfn)

def updateHash(db, fileid, scanfile):
  hash_md5 = computeHash(scanfile, hashlib.md5)
  if hash_md5:
    db.execute("UPDATE files SET hash_md5=? WHERE id=?", [buffer(hash_md5), fileid])

def addFileEntry(db, scanfile, containerid=None):
  path = scanfile.key
  mtime = scanfile.mtime
  size = scanfile.size
  # TODO: mtime == 0 or mtime > now
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
          sf = ScanFile(joinPaths(arcfile.key, parseUnicode(info.name)), info.size, info.mtime)
          addFileEntry(filesdb, sf, containerid=containerid)

class ZipScanFile(ScanFile):
  def getFileHandle(self):
    return self.zipfile.open(self.zipinfo,'r')

def processZipFile(arcfile, containerid=None):
  with arcfile.getFileHandle() as f:
    with zipfile.ZipFile(f, 'r') as zipf:
      for info in zipf.infolist():
        sf = ZipScanFile(joinPaths(arcfile.key, parseUnicode(info.filename)), info.file_size, info.date_time)
        sf.zipfile = zipf
        sf.zipinfo = info
        addFileEntry(filesdb, sf, containerid=containerid)

def processArchive(arcfile, containerid=None):
  with arcfile.getFileHandle() as f:
    with libarchive.fd_reader(f.fileno()) as archive:
      for entry in archive:
        if entry.isfile:
          sf = ScanFile(joinPaths(arcfile.key, parseUnicode(entry.path)), entry.size, entry.mtime)
          # TODO: getFileHandle
          addFileEntry(filesdb, sf, containerid=containerid)

def processScanFile(scanfile):
  fileinfo = addFileEntry(filesdb, scanfile)
  wasmodified = type(fileinfo) == type(0L)
  # file is added/modified (or force), and has file handle
  if (force or wasmodified) and hasattr(scanfile, 'getFileHandle'):
    fileid = fileinfo if wasmodified else fileinfo[0]
    try:
      fn = scanfile.key
      compressed = False
      if fn.endswith(EXTS_COMPRESS):
        fn = os.path.splitext(fn)[0]
        compressed = True
      if fn.endswith(EXTS_ZIP) and not compressed:
        processZipFile(scanfile, containerid=fileid)
      elif fn.endswith(EXTS_ARCHIVE):
        processArchive(scanfile, containerid=fileid)
    except KeyboardInterrupt:
      filesdb.rollback()
      raise
    except:
      print 'ERROR:',sys.exc_info()[1]
      #traceback.print_exc(file=sys.stderr)
      filesdb.execute("UPDATE files SET errors=? WHERE id=?", [str(sys.exc_info()[1]), fileid])


###

def run(args, keywords):
  global filesdb, force, compute_hashes
  force = 'force' in keywords
  if force:
    print "Forced refresh."
  if 'nohash' in keywords:
    compute_hashes = False
    print "No hashes."
  globaldb = openGlobalDatabase(getGlobalDatabasePath(), create=True)
  if len(args)==0:
    print "Must specify at least one collection."
    return False
  for arg in args:
    cloc = parseCollectionLocation(globaldb, arg)
    print "Scanning %s" % (str(cloc))
    filesdb = openFileDatabase(cloc.getFileDatabasePath(), create=True)
    scanres = ScanResults(cloc)
    scanner = FileScanner(cloc.url)
    for scanfile in scanner.scan():
      processScanFile(scanfile)
      filesdb.commit()
    print "Scan complete, updating database..."
    numdeleted = scanres.deleteFilesNotSeenSince(filesdb, sessionStartTime)
    numorphaned = scanres.deleteOrphanedFiles(filesdb)
    if numdeleted or numorphaned:
      print "%d files removed, %d orphaned" % (numdeleted, numorphaned)
    scanres.updateFromFilesTable(filesdb)
    print scanres
    scanres.addToScansTable(globaldb)
    print "Done."
    filesdb.close()
  globaldb.close()
  return True

###

if __name__ == '__main__':
  import doctest, scan
  doctest.testmod(scan)
