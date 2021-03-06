#!/usr/bin/python

import sys,os.path,json,datetime,time,sqlite3,locale,urllib,codecs,calendar
import platform,socket
import uuid,urlparse
import fnmatch,traceback
from mount import mountInfo

# set UTF-8 locale
# TODO: non-english
locale.setlocale(locale.LC_ALL, ('en', 'utf-8'))

# TODO: set encoding
reload(sys)  
sys.setdefaultencoding('utf8')
sys.stderr = codecs.getwriter('utf8')(sys.stderr)
sys.stdout = codecs.getwriter('utf8')(sys.stdout)

# TODO?
sessionStartTime = long(time.time())

foldercache = {}

METADIR='.cupaloy'
GLOBALDBFILE='hosts/%s.db'

EXCLUDES=['.cupaloy','.DS_Store','.Spotlight*',u'Icon\uf00d','Icon\r','fseventsd-uuid',
'.Trashes','ehthumbs.db','desktop.ini','Thumbs.db',
'~*','*~','.~lock.*','*.crdownload','@eaDir','@SynoResource','.@__thumb','._*']
INCLUDES=[]

verbose = 0

###

def parseUUID(uuid):
  return str(uuid).lower()

def isIncluded(name):
  """
  >>> INCLUDES.append('foo/*/bar/')
  >>> isIncluded('foo')
  True
  >>> isIncluded('foo/latest')
  True
  >>> isIncluded('foo/latest/barro')
  True
  >>> isIncluded('foo/latest/bar/')
  True
  >>> isIncluded('foo/latest/barro/')
  False
  >>> isIncluded('foo/latest/glozz/')
  False
  """
  inccount = 0
  for inc in INCLUDES:
    # only evaluate includes if # of path separators is >= that of file path
    if name.count(os.sep) >= inc.count(os.sep):
      if fnmatch.fnmatch(name, inc):
        if verbose>1:
          print "included: %s (%s)" % (name, inc)
        return not isExcluded(name)
      inccount += 1
  if inccount:
    if verbose>1:
      print "not included: %s" % name
    return False
  else:
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
      if verbose>1:
        print "excluded: %s (%s)" % (name, ex)
      return True
  return False

def cleanFilename(fn):
  """
  >>> cleanFilename('/foo/bar')
  '%2Ffoo%2Fbar'
  >>> cleanFilename('file:///users/foo/bar/Mega_mega-mega')
  'file%3A%2F%2F%2Fusers%2Ffoo%2Fbar%2FMega_mega-mega'
  """
  # TODO: lowercase?
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

# rename file so that case is correct
def fixFileCase(filepath):
  if os.path.exists(filepath):
    os.rename(filepath,filepath+'.tmp')
    os.rename(filepath+'.tmp',filepath)

def getHomeMetaDir():
  homedir = os.environ.get('CUPALOY_HOME') or os.environ['HOME']
  return os.path.join(homedir, METADIR)

def setHomeMetaDir(dir):
  assert os.path.isdir(dir)
  os.environ['CUPALOY_HOME'] = dir

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

def getAllCollectionLocations(args):
  result = {}
  allurls = set()
  # find all collection locations
  for dbfn in getAllHostDBFiles():
    db = openGlobalDatabase(os.path.join(getHomeMetaDir(), 'hosts', dbfn))
    if not args:
      args = [''] # TODO: not the fastest way
    for arg in args:
      clocs = parseCollectionLocations(db, arg)
      for cl in clocs:
        if not cl.url in allurls:
          try:
            result[cl.collection.uuid].append(cl)
          except KeyError:
            result[cl.collection.uuid] = [cl]
          allurls.add(cl.url)
    db.close()
  return result

def addMergedVolumeDatabase(mergedb):
  first = True
  for dbfn in getAllHostDBFiles():
    dbpath = os.path.join(getHomeMetaDir(), 'hosts', dbfn)
    assert os.path.getsize(dbpath)
    mergedb.execute("ATTACH DATABASE ? AS db", [dbpath])
    try:
      if first:
        sql = "CREATE TABLE volumes AS "
      else:
        sql = "INSERT INTO volumes "
      sql += "SELECT * FROM db.volumes"
      mergedb.execute(sql)
      first = False
    except sqlite3.OperationalError: # TODO?
      traceback.print_exc(file=sys.stderr)
    finally:
      mergedb.execute("DETACH DATABASE db")
  return mergedb
    
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
    hash1 BINARY,
    hash2 BINARY,
    is_real BOOL,
    has_errors BOOL
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
      fdp = loc.getFileDatabasePath()
      if not os.path.exists(fdp):
        print "*** No db file for %s" % loc
        continue
      if os.path.getsize(fdp) <= 0:
        print "*** Empty db file for %s" % loc
        continue
      mergedb.execute("ATTACH DATABASE ? AS db", [fdp])
      try:
        # add only real files
        mergedb.execute("""
        INSERT INTO files
        SELECT ?,?,path,name,size,modtime,hash1,hash2,
            file_id IS NULL AS is_real,
            io_errors IS NOT NULL OR fmt_errors IS NOT NULL AS has_errors
          FROM db.files f
          JOIN db.folders p ON p.id=f.folder_id
         WHERE is_real IN (%s)
        """ % (realvirt), [locidx, collidx])
        #print realvirt,include_real,include_virtual,include_real+include_virtual
        locset.add(loc)
        print locidx,loc
        locidx += 1
      except sqlite3.OperationalError: # TODO?
        traceback.print_exc(file=sys.stderr)
      finally:
        mergedb.execute("DETACH DATABASE db")
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
    self.uuid = parseUUID(uuid)
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
    #return "%s (%s)" % (self.name, self.uuid)
    return "%s" % (self.name)

class CollectionLocation:

  def __init__(self, collection, url, locname=None, scantime=None, includes=None, excludes=None, volume=None):
    self.collection = collection
    self.url = url
    self.volume = volume
    # TODO?
    #if not volume:
    #  self.volume = getVolumeFromURL(url)
    self.locname = locname
    self.scantime = scantime
    self.includes = includes
    self.excludes = excludes

  def applyIncludes(self):
    global INCLUDES,EXCLUDES
    self.old_includes = INCLUDES
    self.old_excludes = EXCLUDES
    if self.includes:
      INCLUDES = INCLUDES[:]
      INCLUDES.extend(self.includes)
    if self.excludes:
      EXCLUDES = EXCLUDES[:]
      EXCLUDES.extend(self.excludes)
    
  def unapplyIncludes(self):
    global INCLUDES,EXCLUDES
    INCLUDES[:] = self.old_includes
    EXCLUDES[:] = self.old_excludes

  """
  Find the corresponding file database for this collection.
  """
  def getFileDatabasePath(self):
    assert self.url
    fn = '%s.db' % (cleanFilename(self.url))
    return os.path.join(getHomeMetaDir(), 'collections', self.collection.uuid, fn)

  def updateVolume(self, db):
    if self.volume:
      v = self.volume
      db.execute("""
        REPLACE INTO volumes
        (vol_uuid,vol_label,disk_uuid,disk_label,mediatype,fstype,mount_point,last_seen_from,last_seen_time,usage)
        VALUES (?,?,?,?,?,?,?,?,?,?)
      """, [
        v.vol_uuid, v.vol_label, v.disk_uuid, v.disk_label,
        v.mediatype, v.fstype, v.mount_point,
        getNodeName(), sessionStartTime, 'unknown'
      ])
      db.commit()
    
  def __repr__(self):
    if self.locname:
      return "%s @ %s (%s)" % (self.collection, self.locname, self.url)
    else:
      return "%s @ %s" % (self.collection, self.url)


"""
Load a collection definition from a metadata directory.
"""
def loadCollectionLocation(dir):
  cfgfn = os.path.join(dir, METADIR, Collection.METAFILENAME)
  if os.path.exists(cfgfn):
    with open(cfgfn,'r') as inf:
      obj = json.load(inf)
      volume = mountInfo.getVolumeAt(dir)
      url = getURLForVolume(volume, dir)
      return CollectionLocation(Collection(obj['uuid'], obj['name']), url, 
        includes=obj.get('includes'), excludes=obj.get('excludes'), volume=volume)
  else:
    raise Exception("Could not find collection at %s" % dir)

"""
Returns a file URL of the form "file://nodename/path"
"""
def getURLForVolume(volume, path):
  assert volume
  assert volume.vol_uuid
  assert volume.mount_point
  abspath = os.path.abspath(path)
  if abspath[0:len(volume.mount_point)] == volume.mount_point:
    return 'file://%s' % joinPaths(volume.vol_uuid, abspath[len(volume.mount_point):])
  else:
    # TODO? node name?
    return 'file:///%s' % (abspath)

def getVolumeFromURL(url):
  pr = urlparse.urlparse(url)
  if pr.scheme == 'file' and pr.netloc and len(pr.netloc):
    volume = mountInfo.getVolumeByUUID(pr.netloc)
    return volume
  return None

def getDirectoryFromFileURL(url):
  """
  >>> getDirectoryFromFileURL('file:///tmp')
  '/tmp'
  """
  pr = urlparse.urlparse(url)
  assert pr.scheme == 'file'
  # if file://netloc/, prepend root of mount to path
  # TODO?
  if pr.netloc and len(pr.netloc):
    volume = mountInfo.getVolumeByUUID(pr.netloc)
    if not volume:
      raise Exception("Could not find volume for %s" % pr.netloc)
    return os.path.normpath(os.path.join(volume.mount_point, pr.path[1:]))
  else:
    return pr.path

def getCollectionLocationsFromDB(globaldb):
  # select most recent scan
  rows = globaldb.execute("""
  SELECT collection_uuid,collection_name,url,last_scanned_from,last_scan_time
  FROM locations
  GROUP BY collection_uuid,url
  """)
  # TODO: timestamp?
  return [CollectionLocation(Collection(x,y),z,h,t) for x,y,z,h,t in rows]

"""
Find matching collections from a directory path, URL or (partial) name.
"""
def parseCollectionLocations(globaldb, arg):
  # if it's a directory, return it
  if len(arg)>0 and os.path.isdir(arg):
    return [ loadCollectionLocation(arg) ]
  # match the string against recently scanned collections
  # TODO: match with config file?
  rows = globaldb.execute("""
  SELECT collection_uuid,collection_name,url,last_scanned_from,last_scan_time
  FROM locations
  WHERE collection_uuid=? OR collection_name=? or url=? 
  GROUP BY collection_uuid,url
  ORDER BY last_scan_time DESC
  """, [arg, arg, arg]).fetchall()
  # exact match failed? prefix match
  if len(rows)==0:
    rows = globaldb.execute("""
    SELECT collection_uuid,collection_name,url,last_scanned_from,last_scan_time
    FROM locations
    WHERE collection_uuid LIKE ?||'%' OR collection_name LIKE ?||'%' OR url LIKE ?||'%'
    GROUP BY collection_uuid,url
    ORDER BY last_scan_time DESC
    """, [arg, arg, arg]).fetchall()
  return [CollectionLocation(Collection(x,y),z,h,t) for x,y,z,h,t in rows]

"""
Find a single collection from a directory path, URL or (partial) name.
"""
def parseCollectionLocation(globaldb, arg, disambiguate=False):
  results = parseCollectionLocations(globaldb, arg)
  if len(results) == 0:
    raise Exception( "Could not find collection for '%s'." % (arg) )
  elif len(results) > 1 and not disambiguate:
    raise Exception( "Multiple matching collections for '%s' (%s)." % (arg, results) )
  else:
    return results[0]

def makeDirsFor(path):
  dir = os.path.dirname(path)
  if not os.path.exists(dir):
    try:
      os.makedirs(dir)
    except:
      pass

def executeSQLList(db, stmts):
  userVersion = db.execute("PRAGMA user_version").fetchone()[0]
  maxVersion = userVersion
  if verbose:
    print 'user_version %d' % userVersion
  for sql in stmts:
    sql = sql.strip()
    if sql[0] == '>':
      version,sql = sql[1:].split(None, 1)
      version = int(version)
      if version <= userVersion:
        continue
      maxVersion = max(maxVersion, version)
    db.execute(sql)
  if maxVersion > userVersion:
    if verbose:
      print 'upgraded to user_version %d' % maxVersion
    db.execute("PRAGMA user_version = %d" % maxVersion)

def openGlobalDatabase(filepath, create=False):
  if create:
    makeDirsFor(filepath)
    fixFileCase(filepath)
  else:
    assert os.path.exists(filepath)
  db = sqlite3.connect(filepath)
  if create:
    executeSQLList(db, ["""
    >2 CREATE TABLE IF NOT EXISTS scans (
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
      total_real_size REAL,
      hash_metadata TEXT
    )
    ""","""
    >1 ALTER TABLE scans ADD COLUMN vol_uuid TEXT
    ""","""
    >2 CREATE TABLE IF NOT EXISTS volumes (
      vol_uuid TEXT PRIMARY KEY,
      vol_label TEXT,
      disk_uuid TEXT,
      disk_label TEXT,
      mediatype TEXT,
      fstype TEXT,
      mount_point TEXT,
      last_seen_from TEXT,
      last_seen_time LONG,
      usage TEXT
    )
    ""","""
    >4 CREATE TABLE IF NOT EXISTS locations (
      url TEXT PRIMARY KEY,
      collection_uuid TEXT,
      collection_name TEXT,
      vol_uuid TEXT,
      last_scanned_from TEXT,
      last_scan_time LONG
    )
    """])
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
    self.vol_uuid = urlparse.urlparse(cloc.url).netloc # TODO: from cloc?
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

  def __repr__(self):
    #return "%s/%s real/virtual files, %s modified, %s added, %s deleted, %s bytes" % (self.num_real_files, self.num_virtual_files, self.num_modified, self.num_added, self.num_deleted, self.total_real_size)
    return "%s/%s real/virtual files, %s bytes" % (self.num_real_files, self.num_virtual_files, self.total_real_size)
  
  def updateFromFilesTable(self, db):
    self.end_time = long(time.time())
    row = db.execute("""
    SELECT COUNT(file_id),COUNT(*),MIN(modtime),MAX(modtime),
      SUM(CASE WHEN file_id IS NULL THEN 1.0*size ELSE 0 END)
    FROM files f
    LEFT OUTER JOIN folders p ON f.folder_id=p.id
    """, []).fetchone()
    self.num_virtual_files = long(row[0])
    self.num_real_files = long(row[1]) - self.num_virtual_files
    self.min_mtime = long(row[2])
    self.max_mtime = long(row[3])
    self.total_real_size = float(row[4])
  
  def addToScansTable(self, db):
    db.execute("""
      INSERT INTO scans
        (scanned_from,uuid,name,url,vol_uuid,start_time,end_time,
        num_real_files,num_virtual_files,num_modified,num_added,num_deleted,
        min_mtime,max_mtime,total_real_size,hash_metadata)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, [
      getNodeName(),
      self.collection.uuid, self.collection.name, self.url, self.vol_uuid,
      self.start_time, self.end_time,
      self.num_real_files, self.num_virtual_files,
      self.num_modified, self.num_added, self.num_deleted,
      self.min_mtime, self.max_mtime,
      self.total_real_size,
      self.hash_metadata,
    ])
    db.execute("""
      REPLACE INTO locations
        (last_scanned_from,last_scan_time,collection_uuid,collection_name,url,vol_uuid)
      VALUES (?,?,?,?,?,?)
    """, [
      getNodeName(), self.start_time,
      self.collection.uuid, self.collection.name, self.url, self.vol_uuid
    ])
    db.commit()

  def deleteFilesNotSeenSince(self, db, t):
    cur = db.cursor()
    # add to history table
    cur.execute("""
    INSERT INTO history (scan_time,id,folder_id,name,size,modtime,lastseentime,hash1,hash2,io_errors,fmt_errors)
    SELECT ?,f.id,folder_id,name,size,modtime,lastseentime,hash1,hash2,io_errors,fmt_errors FROM files f
      JOIN folders p ON p.id=f.folder_id
      WHERE p.file_id IS NULL
      AND lastseentime < ?
    """, [t, t])
    # delete from files table
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
    fixFileCase(filepath)
  else:
    assert os.path.exists(filepath)
  db = sqlite3.connect(filepath)
  db.execute('PRAGMA journal_mode = MEMORY')
  db.execute('PRAGMA synchronous = OFF')
  db.execute('PRAGMA page_size = 4096')
  if create:
    executeSQLList(db, ["""
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
      hash1 BINARY,
      hash2 BINARY,
      io_errors TEXT,
      fmt_errors TEXT
    )
    ""","""
    CREATE UNIQUE INDEX IF NOT EXISTS files_idx ON files(name,folder_id)
    ""","""
    >2 CREATE TABLE IF NOT EXISTS history (
      scan_time LONG NOT NULL,
      id INTEGER NOT NULL,
      folder_id INTEGER NOT NULL,
      name TEXT NOT NULL,
      size LONG,
      modtime LONG,
      lastseentime LONG,
      hash1 BINARY,
      hash2 BINARY,
      io_errors TEXT,
      fmt_errors TEXT
    )
    """])
  return db

# TODO: this should be unneccessary
def clearFolderCache():
  foldercache.clear()

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
  >>> fixTimestamp(datetime.datetime(2001,2,3,4,5,6))
  981173106L
  """
  if type(ts) == datetime.datetime:
    return long(calendar.timegm(ts.timetuple()))
  elif type(ts) == type((0,)):
    try:
      dt = apply(datetime.datetime, ts)
      return long(calendar.timegm(dt.timetuple()))
    except:
      print (ts, sys.exc_info())
      return None
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
    sys.stderr.write("Error decoding %s: %s\n" % ((s,), sys.exc_info()[1]))
    if verbose:
      traceback.print_exc(file=sys.stderr)
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
