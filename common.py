#!/usr/bin/python

import os.path,json,datetime,time,sqlite3

# TODO?
sessionStartTime = long(time.time())

METADIR='.cupaloy'
GLOBALDBFILE='scans.db'

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
      uuid TEXT,
      name TEXT,
      url TEXT,
      start_time LONG,
      end_time LONG,
      num_files LONG,
      num_added LONG,
      num_removed LONG,
      num_modified LONG,
      min_mtime LONG,
      max_mtime LONG,
      total_size LONG,
      hash_metadata TEXT
    )
    """]
    for sql in stmts:
      db.execute(sql)
  return db

class ScanResults:
  def __init__(self, collection, url):
    self.collection = collection
    self.url = url
    self.start_time = sessionStartTime
    self.end_time = None
    self.num_files = None
    self.num_added = None
    self.num_removed = None
    self.num_modified = None
    self.min_mtime = None
    self.max_mtime = None
    self.total_size = None
    self.hash_metadata = None
  
  def updateFromFilesTable(self, db):
    row = db.execute("""
    SELECT COUNT(*),MIN(modtime),MAX(modtime),SUM(size)
    FROM files f
    JOIN folders p ON f.id=p.id AND file_id IS NULL
    """).fetchone()
    self.num_files = long(row[0])
    self.min_mtime = long(row[1])
    self.max_mtime = long(row[2])
    self.total_size = long(row[3])
  
  def addToScansTable(self, db):
    values = [
      self.collection.uuid, self.collection.name, self.url,
      self.start_time, self.end_time,
      self.num_files, self.num_added, self.num_removed, self.num_modified,
      self.min_mtime, self.max_mtime,
      self.total_size,
      self.hash_metadata
    ]
    db.execute("INSERT INTO scans VALUES (?,?,?,?,?, ?,?,?,?,?, ?,?,?)", values)
    db.commit()

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
    cur.execute("UPDATE files SET lastseentime=? WHERE folder_id=? AND name=?", [sessionStartTime, folderid, filename])
    return fileinfo
  else:
    print (folderid, folderpath, filename, size, mtime)
    cur.execute("INSERT OR REPLACE INTO files (folder_id,name,size,modtime,lastseentime,errors) VALUES (?,?,?,?,?,?)", [folderid, filename, size, mtime, sessionStartTime, None])
    return long(cur.lastrowid)


###

if __name__ == '__main__':
  import doctest, common
  doctest.testmod(common)
