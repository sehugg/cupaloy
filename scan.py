#!/usr/bin/python

import sys,os,os.path,time,traceback,urlparse
import tarfile,zipfile
import sqlite3
import libarchive
import hashlib
from common import *
from progress import ProgressTracker
from contextlib import closing

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

# get scanner for URL

def getScannerForURL(url):
  if url.startswith('file:'):
    from scanner_file import *
    return FileScanner(url)
  elif url.startswith('s3:'):
    from scanner_aws_s3 import *
    return AWSS3Scanner(url)
  else:
    raise Exception("Unrecognized URL: %s" % url)

###

EXTS_TAR = ('.tar','.tgz','.tbz2','.tar.gz','.tar.bz2','tar.z')
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
rescan = False
compute_hashes = True
use_longer_hash = False # TODO: option

def computeHashOfFile(f, digests, bufsize=0x1000):
  """
  >>> import binascii
  >>> computeHashOfFile(open('tests/files/root/emptyfile.txt','r'), [hashlib.md5()])
  >>> binascii.hexlify(computeHashOfFile(open('tests/files/archives/root/rabin.tgz.bz2','r'), [hashlib.md5()])[0])
  '704ca9f2a35f52960f4f94926991241b'
  """
  arr = f.read(bufsize)
  if len(arr)==0:
    return None # empty file, no hash
  while arr:
    for m in digests:
      m.update(arr)
    arr = f.read(bufsize)
  return [m.digest() for m in digests]

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

def updateHash(db, fileid, scanfile, containerid=None):
  try:
    hashes = computeHash(scanfile, [hashlib.sha512()])
    if hashes and len(hashes):
      hash1 = hashes[0][0:16]
      if use_longer_hash:
        hash2 = hashes[0][16:]
        db.execute("UPDATE files SET hash1=?,hash2=? WHERE id=?", [buffer(hash1), buffer(hash2), fileid])
      else:
        db.execute("UPDATE files SET hash1=?,hash2=NULL WHERE id=?", [buffer(hash1), fileid])
  except KeyboardInterrupt:
    raise
  except:
    db.execute("UPDATE files SET hash1=NULL,hash2=NULL WHERE id=?", [fileid])
    # log I/O error if we are NOT part of an archive -- i.e. a real file
    if containerid:
      raise
    else:
      setScanErrorFromException(db, 'io', fileid)

def addFileEntry(db, scanfile, containerid=None):
  path = scanfile.key
  mtime = scanfile.mtime
  size = scanfile.size
  # TODO: mtime == 0 or mtime > now
  folderpath,filename = os.path.split(path)
  if containerid is None: # if is a real file, update progress
    progress.inc(filename, size)
  mtime = fixTimestamp(mtime)
  folderid = getFolderID(db, folderpath, fileid=containerid)
  cur = db.cursor()
  fileinfo = not rescan and cur.execute("SELECT id,size,modtime,hash1 FROM files WHERE folder_id=? AND name=? AND size=? AND modtime=? AND io_errors IS NULL", [folderid, filename, size, mtime]).fetchone()
  if fileinfo:
    if not fileinfo[3]: #TODO?
      updateHash(db, fileinfo[0], scanfile, containerid)
    cur.execute("UPDATE files SET lastseentime=? WHERE id=?", [sessionStartTime, fileinfo[0]])
    return fileinfo
  else:
    if verbose:
      print (folderid, folderpath, filename, size, mtime)
    cur.execute("INSERT OR REPLACE INTO files (folder_id,name,size,modtime,lastseentime,io_errors,fmt_errors) VALUES (?,?,?,?,?,?,?)",
      [folderid, filename, size, mtime, sessionStartTime, None, None])
    # TODO: num_added?
    # TODO: scanres.num_modified += 1
    fileid = long(cur.lastrowid)
    updateHash(db, fileid, scanfile, containerid)
    return fileid

class TarScanFile(ScanFile):
  def getFileHandle(self):
    return closing(self.tarfile.extractfile(self.tarinfo))

def processTarFile(arcfile, containerid=None):
  with arcfile.getFileHandle() as f:
    with tarfile.open(fileobj=f, mode='r') as tarf:
      for info in tarf:
        if info.isfile():
          sf = TarScanFile(joinPaths(arcfile.key, parseUnicode(info.name)), info.size, info.mtime)
          sf.tarfile = tarf
          sf.tarinfo = info
          progress.incGoal(info.name, info.size)
          addFileEntry(filesdb, sf, containerid=containerid)

class ZipScanFile(ScanFile):
  def getFileHandle(self):
    return self.zipfile.open(self.zipinfo,'r')

def processZipFile(arcfile, containerid=None):
  with arcfile.getFileHandle() as f:
    with zipfile.ZipFile(f, 'r') as zipf:
      infolist = zipf.infolist()
      progress.pushGoal(len(infolist), arcfile.size)
      try:
        for info in infolist:
          if not info.filename.endswith('/'): # is a file, in other words...
            sf = ZipScanFile(joinPaths(arcfile.key, parseUnicode(info.filename)), info.file_size, info.date_time)
            sf.zipfile = zipf
            sf.zipinfo = info
            progress.inc(info.filename, info.compress_size)
            addFileEntry(filesdb, sf, containerid=containerid)
      finally:
        progress.popGoal()

def processArchive(arcfile, containerid=None):
  with arcfile.getFileHandle() as f:
    with libarchive.fd_reader(f.fileno()) as archive:
      for entry in archive:
        if entry.isfile:
          sf = ScanFile(joinPaths(arcfile.key, parseUnicode(entry.path)), entry.size, entry.mtime)
          # TODO: getFileHandle
          progress.incGoal(entry.path, entry.size)
          addFileEntry(filesdb, sf, containerid=containerid)

def setScanErrorFromException(filesdb, type, fileid):
  msg = str(sys.exc_info()[1])
  print 'ERROR:',type,msg
  if verbose:
    traceback.print_exc(file=sys.stderr)
  setScanError(filesdb, type, fileid, msg)

def setScanError(filesdb, type, fileid, msg):
  filesdb.execute("UPDATE files SET %s_errors=? WHERE id=?" % type, [msg, fileid])

def processScanFile(scanfile):
  fileinfo = addFileEntry(filesdb, scanfile)
  wasmodified = type(fileinfo) == type(0L)
  # file is added/modified (or rescan), and has file handle
  if (rescan or wasmodified) and hasattr(scanfile, 'getFileHandle'):
    fileid = fileinfo if wasmodified else fileinfo[0]
    fn = scanfile.key.lower()
    try:
      compressed = False
      if fn.endswith(EXTS_TAR):
        processTarFile(scanfile, containerid=fileid)
      else:
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
      # log fmt error if we were reading an archive when error occured
      # TODO: what if error caused by database?
      setScanErrorFromException(filesdb, 'fmt', fileid)

###

def run(args, keywords):
  global filesdb, force, rescan, compute_hashes
  global progress, scanres
  force = 'force' in keywords # TODO?
  rescan = 'rescan' in keywords
  if rescan:
    print "Rescanning."
  if 'nohash' in keywords:
    compute_hashes = False
    print "No hashes."
  globaldb = openGlobalDatabase(getGlobalDatabasePath(), create=True)
  if len(args)==0:
    print "Must specify at least one collection."
    return False
  numGood = 0
  for arg in args:
    cloc = None
    try:
      cloc = parseCollectionLocation(globaldb, arg)
    except:
      print sys.exc_info()[1] # TODO
      continue
    cloc.applyIncludes()
    filesdb = openFileDatabase(cloc.getFileDatabasePath(), create=True)
    numfiles,totalsize = filesdb.execute("SELECT COUNT(*),SUM(size) FROM files JOIN folders ON folder_id=folders.id AND file_id IS NULL").fetchone()
    progress = ProgressTracker()
    if numfiles and totalsize:
      progress.pushGoal(numfiles,totalsize)
    scanres = ScanResults(cloc)
    scanner = getScannerForURL(cloc.url)
    print "Scanning %s" % (str(cloc))
    num_real_files = 0
    for scanfile in scanner.scan():
      if scanfile:
        processScanFile(scanfile)
        filesdb.commit()
        num_real_files += 1
    if num_real_files <= 0:
      print "No files found."
      continue
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
    cloc.unapplyIncludes()
    numGood += 1
  globaldb.close()
  return numGood > 0

###

if __name__ == '__main__':
  import doctest, scan
  doctest.testmod(scan)
