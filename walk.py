#!/usr/bin/python

import sys,os,os.path,datetime,time
import tarfile,zipfile
import sqlite3
import libarchive

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

METADIR='.arc'

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

sessionStartTime = long(time.time())

def openDatabase(filepath, create=False):
  db = sqlite3.connect(filepath)
  db.execute('PRAGMA journal_mode = MEMORY')
  db.execute('PRAGMA synchronous = OFF')
  db.execute('PRAGMA page_size = 4096')
  if create:
    stmts = ["""
    CREATE TABLE IF NOT EXISTS folders (
      id INTEGER NOT NULL PRIMARY KEY,
      path TEXT NOT NULL,
      fileid INTEGER
    )
    ""","""
    CREATE INDEX IF NOT EXISTS folders_idx ON folders(path)
    ""","""
    CREATE TABLE IF NOT EXISTS files (
      id INTEGER NOT NULL PRIMARY KEY,
      folderid INTEGER NOT NULL,
      name TEXT NOT NULL,
      size LONG,
      mtime LONG,
      lastseen LONG,
      errors TEXT
    )
    ""","""
    CREATE UNIQUE INDEX IF NOT EXISTS files_idx ON files(name,folderid)
    """]
    for sql in stmts:
      db.execute(sql)
  return db

maindb = None
foldercache = {}

def joinPaths(a, b):
  return (a + '/' + b).replace('//','/')

def getFolderID(db, folderpath, create=False, fileid=None):
  id = foldercache.get(folderpath)
  if id:
    return id
  row = db.execute("SELECT id FROM folders WHERE path=?", [folderpath]).fetchone()
  if row:
    id = row[0]
  else:
    cur = db.cursor()
    cur.execute("INSERT INTO folders (path,fileid) VALUES (?,?)", [folderpath, fileid])
    id = cur.lastrowid
  foldercache[folderpath] = id
  return id

def addFileEntry(path, size, mtime, containerid=None):
  if type(path) != type(u''):
    try:
      path = path.decode('UTF-8')
    except UnicodeEncodeError:
      print sys.exc_info()
      path = path.decode('cp1252') #TODO?
  folderpath,filename = os.path.split(path)
  mtime = fixTimestamp(mtime)
  folderid = getFolderID(maindb, folderpath, fileid=containerid)
  print (folderid, folderpath, filename, size, mtime)
  cur = maindb.cursor()
  cur.execute("INSERT OR REPLACE INTO files (folderid,name,size,mtime,lastseen,errors) VALUES (?,?,?,?,?,?)", [folderid, filename, size, mtime, sessionStartTime, None])
  return cur.lastrowid

def fixTimestamp(ts):
  if type(ts) == type((0,)):
    dt = apply(datetime.datetime, ts)
    return long(time.mktime(dt.timetuple()))
  elif ts == None:
    return None
  else:
    return long(ts)

def processTarFile(containerKey, path, containerid=None):
  with tarfile.open(path, 'r') as tarf:
    for info in tarf:
      if info.isfile():
        addFileEntry( joinPaths(containerKey, info.name), info.size, info.mtime, containerid=containerid )

def processZipFile(containerKey, path, containerid=None):
  with zipfile.ZipFile(path, 'r') as zipf:
    for info in zipf.infolist():
      addFileEntry( joinPaths(containerKey, info.filename), info.file_size, info.date_time, containerid=containerid )

def processArchive(containerKey, path, containerid=None):
  with libarchive.file_reader(path) as archive:
    for entry in archive:
      if entry.isfile:
        addFileEntry( joinPaths(containerKey, entry.path), entry.size, entry.mtime, containerid=containerid )

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
  fileid = addFileEntry(key, size, mtime)
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
      maindb.commit()

def findRootDir(path):
  if len(path)<2:
    return None

  if os.path.isdir(path):
    metaDir = os.path.join(path, METADIR)
    if os.path.isdir(metaDir):
      return os.path.dirname(metaDir)

  return findRootDir(os.path.dirname(path))

###

rootDir = findRootDir(sys.argv[1])
if not rootDir:
  print "No %s directory found!" % (METADIR)
  sys.exit(1)

metaDir = os.path.join(rootDir, METADIR)

maindb = openDatabase(os.path.join(metaDir, 'files.db'), create=True)

walkDirectory(rootDir, sys.argv[1])
