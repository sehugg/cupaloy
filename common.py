#!/usr/bin/python

import sys,os.path,json,datetime,time,sqlite3,locale,urllib
import platform,socket
import uuid,urlparse
import fnmatch
from mount import *

# set UTF-8 locale
# TODO: non-english
locale.setlocale(locale.LC_ALL, ('en', 'utf-8'))

# TODO?
sessionStartTime = long(time.time())

METADIR='.cupaloy'
GLOBALDBFILE='hosts/%s.db'

EXCLUDES=['.cupaloy','*~','.DS_Store','.~lock.*','.Spotlight*']

###

def isIncluded(name):
  return not isExcluded(name)

def isExcluded(name):
  """
  >>> isExcluded("foo/foo~")
  True
  >>> isExcluded("foo~")
  True
  >>> isExcluded("foo/.cupaloy")
  False
  """
  for ex in EXCLUDES:
    if fnmatch.fnmatch(name, ex):
      return True
  return False

def cleanFilename(fn):
  """
  >>> cleanFilename('/foo/bar')
  '%2Ffoo%2Fbar'
  >>> cleanFilename('file:///users/foo/bar/mega_mega-mega')
  'file%3A%2F%2F%2Fusers%2Ffoo%2Fbar%2Fmega_mega-mega'
  """
  return urllib.quote(fn, safe='')

def joinPaths(a, b):
  return (a + '/' + b).replace('//','/')

suffixes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
def humansize(nbytes):
    if nbytes == 0: return '0 B'
    i = 0
    while nbytes >= 1024 and i < len(suffixes)-1:
        nbytes /= 1024.
        i += 1
    f = ('%.2f' % nbytes).rstrip('0').rstrip('.')
    return '%s %s' % (f, suffixes[i])

###

def getHomeMetaDir():
  homedir = os.environ.get('CUPALOY_HOME') or os.environ['HOME']
  return os.path.join(homedir, METADIR)

def getGlobalDatabasePath(host=None):
  dir = getHomeMetaDir()
  if not os.path.exists(dir):
    os.mkdir(dir)
  if not host:
    host = getNodeName()
  return os.path.join(dir, GLOBALDBFILE % cleanFilename(host))

def getAllHostDBFiles():
  return os.listdir(os.path.join(getHomeMetaDir(), "hosts"))

def getAllCollectionUUIDs():
  return os.listdir(os.path.join(getHomeMetaDir(), "collections"))

def getAllCollectionLocations():
  clocs = {}
  # find all collection locations
  for dbfn in getAllHostDBFiles():
    db = openGlobalDatabase(os.path.join(getHomeMetaDir(), 'hosts', dbfn))
    for cl in getCollectionLocationsFromDB(db):
      try:
        clocs[cl.collection.uuid].append(cl)
      except KeyError:
        clocs[cl.collection.uuid] = [cl]
    db.close()
  return clocs

def getMergedFileDatabase(clocs, include_real=True, include_virtual=False):
  # insert into merged db
  mergedb = sqlite3.connect(":memory:")
  mergedb.execute("""
  CREATE TABLE files (
    locidx INTEGER,
    collidx INTEGER,
    path TEXT,
    name TEXT,
    size LONG,
    modtime LONG,
    hash_md5 BINARY,
    is_real BOOL
  )
  """)
  # TODO: locs from more than one host
  locidx = 0
  collidx = 0
  locset = set()
  realvirt = ['','1','0','0,1'][include_real+(include_virtual*2)] # IN(?) sql clause
  for uuid,locs in clocs.items(): # TODO: order
    for loc in locs:
      if loc in locset:
        continue # TODO?
      locset.add(loc)
      fdp = loc.getFileDatabasePath()
      print locidx,loc
      mergedb.execute("ATTACH DATABASE ? AS db", [fdp])
      # add only real files
      mergedb.execute("""
      INSERT INTO files
      SELECT ?,?,path,name,size,modtime,hash_md5,file_id IS NULL AS is_real
        FROM db.files f
        JOIN db.folders p ON p.id=f.folder_id
       WHERE is_real IN (%s)
      """ % (realvirt), [locidx, collidx])
      #print realvirt,include_real,include_virtual,include_real+include_virtual
      mergedb.execute("DETACH DATABASE db")
      locidx += 1
    collidx += 1
  return mergedb

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

class CollectionLocation:

  def __init__(self, collection, url):
    self.collection = collection
    self.url = url

  """
  Find the corresponding file database for this collection.
  """
  def getFileDatabasePath(self):
    assert self.url
    fn = '%s.db' % (cleanFilename(self.url))
    return os.path.join(getHomeMetaDir(), 'collections', self.collection.uuid, fn)
    
  def __repr__(self):
    return "%s @ %s" % (self.collection, self.url)


"""
Load a collection definition from a metadata directory.
"""
def loadCollectionLocation(dir):
  cfgfn = os.path.join(dir, METADIR, Collection.METAFILENAME)
  if os.path.exists(cfgfn):
    with open(cfgfn,'r') as inf:
      obj = json.load(inf)
      return CollectionLocation(Collection(obj['uuid'], obj['name']), getFileURL(dir))
  else:
    raise Exception("Could not find collection at %s" % dir)
    # TODO: no config file? override name? warning?
    url = getFileURL(dir)
    uid = uuid.uuid3(uuid.NAMESPACE_URL, url)
    name = urlparse.urlparse(url).netloc
    return CollectionLocation(Collection(uid, name), url)

"""
Returns a file URL of the form "file://nodename/path"
"""
def getFileURL(path):
  """
  >>> getFileURL("/tmp")
  'file://a06997a7-9a7a-4395-9aa2-8630f3eb13b2/tmp'
  >>> getFileURL("/")
  'file://a06997a7-9a7a-4395-9aa2-8630f3eb13b2/'
  >>> getFileURL("/boot/efi")
  'file://CB77-C81C/'
  >>> getFileURL("/boot/efi/foo")
  'file://CB77-C81C/foo'
  """
  assert(len(path)>0)
  vol_uuid,vol_mount = mountInfo.forPath(path)
  abspath = os.path.abspath(path)
  if abspath[0:len(vol_mount)] == vol_mount:
    return 'file://%s' % joinPaths(vol_uuid, abspath[len(vol_mount):])
  else:
    # TODO? node name?
    return 'file:///%s' % (abspath)

def getCollectionLocationsFromDB(globaldb):
  # select most recent scan
  rows = globaldb.execute("""
  SELECT DISTINCT uuid,name,url,MAX(start_time)
  FROM scans
  GROUP BY uuid,url
  """)
  return [CollectionLocation(Collection(x,y),z) for x,y,z,t in rows]  

"""
Find matching collections from a directory path, URL or (partial) name.
"""
def parseCollectionLocations(globaldb, arg):
  # if it's a directory, return it
  if os.path.isdir(arg):
    return [ loadCollectionLocation(arg) ]
  # match the string against recently scanned collections
  # TODO: match with config file?
  rows = globaldb.execute("""
  SELECT DISTINCT uuid,name,url FROM scans
  WHERE uuid LIKE ?||'%' OR name LIKE ?||'%' OR url LIKE ?||'%'
  ORDER BY start_time DESC
  """, [arg, arg, arg])
  return [CollectionLocation(Collection(x,y),z) for x,y,z in rows]

"""
Find a single collection from a directory path, URL or (partial) name.
"""
def parseCollectionLocation(globaldb, arg, disambiguate=True):
  results = parseCollectionLocations(globaldb, arg)
  if len(results) == 0:
    raise Exception( "Could not find collection for '%s'." % (arg) )
  elif len(results) > 1 and not disambiguate:
    raise Exception( "Multiple matching collections for '%s'." % (arg) )
  else:
    return results[0]

def makeDirsFor(path):
  dir = os.path.dirname(path)
  if not os.path.exists(dir):
    try:
      os.makedirs(dir)
    except:
      pass

def openGlobalDatabase(filepath, create=False):
  if create:
    makeDirsFor(filepath)
  else:
    assert os.path.exists(filepath)
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
  def __init__(self, cloc):
    assert cloc
    self.collection = cloc.collection
    self.url = cloc.url
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
  if create:
    makeDirsFor(filepath)
  else:
    assert os.path.exists(filepath)
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
      errors TEXT,
      hash_md5 BINARY
    )
    ""","""
    --ALTER TABLE files ADD COLUMN hash_md5 BINARY
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

###

def parseUnicode(s):
  """
  >>> parseUnicode('foo\xcc\x81')
  u'foo\\u0301'
  """
  if type(s) == type(u''):
    return s
  try:
    return unicode(s, 'UTF-8')
  except:
    sys.stderr.write("'%s': %s'\n" % (s, sys.exc_info()))
    # http://stackoverflow.com/questions/18648154/read-lines-of-a-textfile-and-getting-charmap-decode-error
    # TODO?
    try:
      return s.decode('cp1252')
    except:
      return s.decode('cp850')

class ScanFile:

  def __init__(self, key, size, mtime):
    self.key = parseUnicode(key)
    self.size = long(size) if size is not None else None
    mtime = fixTimestamp(mtime)
    # filter out too soon or future times
    if mtime < 2 or mtime > sessionStartTime+(86400*365):
      mtime = None
    self.mtime = mtime

  def __repr__(self):
    return str((self.key, self.size, self.mtime))


###

if __name__ == '__main__':
  import doctest, common
  doctest.testmod(common)
