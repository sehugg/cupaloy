#!/usr/bin/python

import sys,os,os.path,datetime,time
import tarfile,zipfile
import sqlite3

METADIR='.arc'

EXTS_TAR = ('.tar','.tgz','.tbz2','.tar.gz','.tar.bz2')
EXTS_ZIP = ('.zip')

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
      virtual BOOL DEFAULT FALSE
    )
    ""","""
    CREATE INDEX IF NOT EXISTS folders_idx ON folders(path)
    ""","""
    CREATE TABLE IF NOT EXISTS files (
      id INTEGER NOT NULL PRIMARY KEY,
      pid INTEGER NOT NULL,
      name TEXT NOT NULL,
      size LONG,
      mtime LONG,
      lastseen LONG
    )
    ""","""
    CREATE INDEX IF NOT EXISTS files_idx ON files(pid,name)
    """]
    for sql in stmts:
      db.execute(sql)
  return db

maindb = None
foldercache = {}

def getFolderID(db, folderpath, create=False, virtual=False):
  id = foldercache.get(folderpath)
  if id:
    return id
  row = db.execute("SELECT id FROM folders WHERE path=?", [folderpath]).fetchone()
  if row:
    id = row[0]
  else:
    cur = db.cursor()
    cur.execute("INSERT INTO folders (path,virtual) VALUES (?,?)", [folderpath, virtual])
    id = cur.lastrowid
  foldercache[folderpath] = id
  return id

def getFileExtension(path):
  i = path.rindex('.')
  return path[i+1:] if i>0 else None

def addFileEntry(containerKey, filename, size, mtime, virtual=False):
  containerKey = containerKey.decode('utf-8')
  filename = filename.decode('utf-8')
  mtime = fixTimestamp(mtime)
  pid = getFolderID(maindb, containerKey, virtual)
  print (pid, containerKey, filename, size, mtime)
  cur = maindb.cursor()
  cur.execute("REPLACE INTO files (pid,name,size,mtime,lastseen) VALUES (?,?,?,?,?)", [containerKey, filename, size, mtime, sessionStartTime])
  return cur.lastrowid

def fixTimestamp(ts):
  if type(ts) == type((0,)):
    dt = apply(datetime.datetime, ts)
    return long(time.mktime(dt.timetuple()))
  else:
    return long(ts)

def processTarFile(containerKey, path):
  with tarfile.open(path, 'r') as tarf:
    for info in tarf:
      if info.isfile():
        addFileEntry( containerKey, info.name, info.size, info.mtime, virtual=True )

def processZipFile(containerKey, path):
  with zipfile.ZipFile(path, 'r') as zipf:
    for info in zipf.infolist():
      addFileEntry( containerKey, info.filename, info.file_size, info.date_time, virtual=True )

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
  addFileEntry(containerKey, filename, size, mtime)
  if filename.endswith(EXTS_TAR):
    processTarFile(key, path)
  elif filename.endswith(EXTS_ZIP):
    processZipFile(key, path)

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
