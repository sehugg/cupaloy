#!/usr/bin/python

import os,os.path,sys,uuid,sqlite3,math
import tabulate
from common import *

def pct(n,d,dups):
  if dups<2:
    return ''
  elif n==None or not d:
    return '-'
  elif n==d:
    return '100%'
  else:
    return "%5.1f%%" % (math.floor(n*1000.0/d)/10.0)

def run(args, keywords):
  clocs = {}
  # find all collection locations
  for dbfn in getAllHostDBFiles():
    db = openGlobalDatabase(os.path.join(getHomeMetaDir(), 'hosts', dbfn))
    for cl in getAllCollectionLocations(db):
      try:
        clocs[cl.collection.uuid].append(cl)
      except KeyError:
        clocs[cl.collection.uuid] = [cl]
    db.close()
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
  uuids = []
  locset = set()
  for uuid,locs in clocs.items(): # TODO: order
    uuids.append(uuid)
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
       WHERE is_real
      """, [locidx, collidx])
      mergedb.execute("DETACH DATABASE db")
      locidx += 1
    collidx += 1
  # select
  #mergedb.execute("CREATE INDEX file_idx_name ON files(collidx,name)")
  mergedb.execute("""
  CREATE TABLE dupfiles AS
    SELECT COUNT(*) as dups,collidx,GROUP_CONCAT(locidx) as locs,path,name,
      MIN(IFNULL(size,-1)) as minsize,
      MAX(IFNULL(size,0)) as maxsize,
      MIN(IFNULL(modtime,-1)) as mintime,
      MAX(IFNULL(modtime,0)) as maxtime,
      MIN(IFNULL(hash_md5,'')) as minhash,
      MAX(IFNULL(hash_md5,'-')) as maxhash
    FROM files
    GROUP BY collidx,path,name
  """)
  results = mergedb.execute("""
  SELECT
    collidx,dups,locs,
    COUNT(*) as nfiles,
    SUM(maxsize) as totsize,
    SUM(minsize=maxsize) as samesize,
    SUM(mintime=maxtime) as sametime,
    SUM(minhash=maxhash) as samehash
  FROM dupfiles
  GROUP BY collidx,dups,locs
  """).fetchall()
  table = [
    (
      clocs[uuids[collidx]][0].collection.name, # TODO: ordering
      dups,
      locs,
      nfiles,
      '%10.2f' % (totsize/(1000.0*1000.0)),
      pct(samesize,nfiles,dups),
      pct(samehash,nfiles,dups),
      pct(sametime,nfiles,dups),
    )
    for collidx,dups,locs,nfiles,totsize,samesize,sametime,samehash in results
  ]
  headers = ["Collection","# Copies","Locations","# Files","Total MB","Size Match","Hash Match","Time Match"]
  print
  print tabulate.tabulate(table, headers=headers)

  if 0:
    for row in mergedb.execute("SELECT dups,collidx,locs,path,name,minsize,maxsize,mintime,maxtime FROM dupfiles WHERE dups=1 ORDER BY collidx,locs,path,name"):
      if isIncluded(row[3]) and isIncluded(row[4]):
        print row
  if 0:
    for row in mergedb.execute("SELECT dups,collidx,locs,path,name,minsize,maxsize,mintime,maxtime FROM dupfiles WHERE mintime!=maxtime ORDER BY collidx,locs,path,name"):
      if isIncluded(row[3]) and isIncluded(row[4]):
        print row
