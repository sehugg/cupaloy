#!/usr/bin/python

import os.path,json,datetime,time,sqlite3
import platform,socket

# TODO?
sessionStartTime = long(time.time())

METADIR='.cupaloy'
GLOBALDBFILE='global.db'

def getHomeMetaDir():
  return os.path.join(os.environ['HOME'], METADIR)
  
def getGlobalDatabasePath():
  dir = getHomeMetaDir()
  if not os.path.exists(dir):
    os.mkdir(dir)
  return os.path.join(dir, GLOBALDBFILE)

###

def findRootDir(path):
  if len(path)<2:
    return None
  if os.path.isdir(path):
    metaDir = os.path.join(path, METADIR)
    if os.path.isdir(metaDir):
      return os.path.dirname(metaDir)
  return findRootDir(os.path.dirname(path))

def getMetaDir(rootDir):
  metaDir = os.path.join(rootDir, METADIR)
  return metaDir

###

class Collection:

  METAFILENAME='config.json'
  
  def __init__(self, uuid, name):
    self.uuid = str(uuid)
    self.name = name
    
  """
  Save the collection's identifying metadata to a metadata directory.
  """
  def write(self, dir, force=False):
    if not os.path.isdir(dir):
      os.mkdir(dir)
    cfgfn = os.path.join(dir, Collection.METAFILENAME)
    exists = os.path.isfile(cfgfn)
    if force or not exists:
      print "Writing '%s'" % (cfgfn)
      with open(cfgfn,'w') as outf:
        json.dump(self.__dict__, outf, sort_keys=True, indent=2)
    else:
      raise Exception("Could not write '%s': file exists" % (cfgfn))
      
  def __repr__(self):
    return "%s (%s)" % (self.name, self.uuid)

"""
Load a collection definition from a metadata directory.
"""
def loadCollection(dir):
  cfgfn = os.path.join(dir, Collection.METAFILENAME)
  with open(cfgfn,'r') as inf:
    obj = json.load(inf)
    return Collection(obj['uuid'], obj['name'])

def openGlobalDatabase(filepath, create=False):
  db = sqlite3.connect(filepath)
  if create:
    stmts = ["""
    CREATE TABLE IF NOT EXISTS scans (
      scanned_from TEXT,
      uuid TEXT,
      name TEXT,
      url TEXT,
      start_time LONG,
      end_time LONG,
      num_real_files LONG,
      num_virtual_files LONG,
      num_modified LONG,
      num_added LONG,
      num_deleted LONG,
      min_mtime LONG,
      max_mtime LONG,
      total_real_size LONG,
      hash_metadata TEXT
    )
    """]
    for sql in stmts:
      db.execute(sql)
  return db

def getNodeName():
  """
  >>> type(getNodeName())
  <type 'str'>
  """
  name = platform.node()
  if not name:
    name = socket.gethostname()
    if not name:
      name = os.environ.get('COMPUTERNAME')
  assert(name)
  return name

class ScanResults:
  def __init__(self, collection, url):
    self.collection = collection
    self.url = url
    self.start_time = sessionStartTime
    self.end_time = None
    self.num_real_files = None
    self.num_virtual_files = None
    self.num_modified = None
    self.num_deleted = None
    self.num_added = None
    self.min_mtime = None
    self.max_mtime = None
    self.total_real_size = None
    self.hash_metadata = None
  
  def updateFromFilesTable(self, db):
    self.end_time = long(time.time())
    row = db.execute("""
    SELECT COUNT(file_id),COUNT(*),MIN(modtime),MAX(modtime),
      SUM(CASE WHEN file_id IS NULL THEN size ELSE 0 END)
    FROM files f
    LEFT OUTER JOIN folders p ON f.folder_id=p.id
    """, []).fetchone()
    self.num_virtual_files = long(row[0])
    self.num_real_files = long(row[1]) - self.num_virtual_files
    self.min_mtime = long(row[2])
    self.max_mtime = long(row[3])
    self.total_real_size = long(row[4])
  
  def addToScansTable(self, db):
    values = [
      getNodeName(),
      self.collection.uuid, self.collection.name, self.url,
      self.start_time, self.end_time,
      self.num_real_files, self.num_virtual_files,
      self.num_modified, self.num_added, self.num_deleted,
      self.min_mtime, self.max_mtime,
      self.total_real_size,
      self.hash_metadata
    ]
    db.execute("INSERT INTO scans VALUES (?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?)", values)
    db.commit()

  def deleteFilesNotSeenSince(self, db, t):
    cur = db.cursor()
    cur.execute("""
    DELETE FROM files WHERE id IN (
      SELECT f.id FROM files f
      JOIN folders p ON p.id=f.folder_id
      WHERE p.file_id IS NULL
      AND lastseentime < ?
    )""", [t])
    db.commit()
    self.num_deleted = cur.rowcount
    cur.close()
    return cur.rowcount
    
  def deleteOrphanedFiles(self, db):
    cur = db.cursor()
    cur.execute("""
    DELETE FROM files WHERE id IN (
      SELECT f.id FROM files f
      JOIN folders p ON p.id=f.folder_id
      WHERE p.file_id IS NOT NULL
      AND NOT EXISTS (
        SELECT * FROM files f2 WHERE f2.id=p.file_id)
    )""")
    db.commit()
    cur.close()
    return cur.rowcount

###

def openFileDatabase(filepath, create=False):
  db = sqlite3.connect(filepath)
  db.execute('PRAGMA journal_mode = MEMORY')
  db.execute('PRAGMA synchronous = OFF')
  db.execute('PRAGMA page_size = 4096')
  if create:
    stmts = ["""
    CREATE TABLE IF NOT EXISTS folders (
      id INTEGER NOT NULL PRIMARY KEY,
      path TEXT NOT NULL,
      file_id INTEGER
    )
    ""","""
    CREATE INDEX IF NOT EXISTS folders_idx ON folders(path)
    ""","""
    CREATE INDEX IF NOT EXISTS folders_idx_2 ON folders(file_id)
    ""","""
    CREATE TABLE IF NOT EXISTS files (
      id INTEGER NOT NULL PRIMARY KEY,
      folder_id INTEGER NOT NULL,
      name TEXT NOT NULL,
      size LONG,
      modtime LONG,
      lastseentime LONG,
      errors TEXT
    )
    ""","""
    CREATE UNIQUE INDEX IF NOT EXISTS files_idx ON files(name,folder_id)
    """]
    for sql in stmts:
      db.execute(sql)
  return db

foldercache = {}

def getFolderID(db, folderpath, create=False, fileid=None):
  id = foldercache.get(folderpath)
  if id:
    return id
  row = db.execute("SELECT id FROM folders WHERE path=?", [folderpath]).fetchone()
  if row:
    id = row[0]
  else:
    cur = db.cursor()
    cur.execute("INSERT INTO folders (path,file_id) VALUES (?,?)", [folderpath, fileid])
    id = cur.lastrowid
  foldercache[folderpath] = id
  return id

def fixTimestamp(ts):
  """
  >>> fixTimestamp(0)
  0L
  """
  if type(ts) == type((0,)):
    dt = apply(datetime.datetime, ts)
    return long(time.mktime(dt.timetuple()))
  elif ts == None:
    return None
  else:
    return long(ts)

def addFileEntry(db, path, size, mtime, containerid=None):
  if type(path) != type(u''):
    try:
      path = path.decode('UTF-8')
    except UnicodeEncodeError:
      print sys.exc_info()
      path = path.decode('cp1252') #TODO?
  folderpath,filename = os.path.split(path)
  mtime = fixTimestamp(mtime)
  folderid = getFolderID(db, folderpath, fileid=containerid)
  cur = db.cursor()
  fileinfo = cur.execute("SELECT id,size,modtime FROM files WHERE folder_id=? AND name=? AND size=? AND modtime=?", [folderid, filename, size, mtime]).fetchone()
  if fileinfo:
    cur.execute("UPDATE files SET lastseentime=? WHERE id=?", [sessionStartTime, fileinfo[0]])
    return fileinfo
  else:
    print (folderid, folderpath, filename, size, mtime)
    cur.execute("INSERT OR REPLACE INTO files (folder_id,name,size,modtime,lastseentime,errors) VALUES (?,?,?,?,?,?)", [folderid, filename, size, mtime, sessionStartTime, None])
    return long(cur.lastrowid)


###

if __name__ == '__main__':
  import doctest, common
  doctest.testmod(common)
