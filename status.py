#!/usr/bin/python

import os,os.path,sys,uuid,sqlite3
import tabulate
from common import *

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
    hash_md5 BINARY
  )
  """)
  # TODO: locs from more than one host
  locidx = 0
  collidx = 0
  uuids = []
  for uuid,locs in clocs.items(): # TODO: order
    uuids.append(uuid)
    for loc in locs:
      fdp = loc.getFileDatabasePath()
      print loc
      mergedb.execute("ATTACH DATABASE ? AS db", [fdp])
      # add only real files
      mergedb.execute("""
      INSERT INTO files
      SELECT ?,?,path,name,size,modtime,hash_md5
        FROM db.files f
        JOIN db.folders p ON p.id=f.folder_id
       WHERE file_id IS NULL
      """, [locidx, collidx])
      mergedb.execute("DETACH DATABASE db")
      locidx += 1
    collidx += 1
  # select
  #mergedb.execute("CREATE INDEX file_idx_name ON files(collidx,name)")
  mergedb.execute("""
  CREATE TABLE dupfiles AS
    SELECT COUNT(*) as dups,collidx,GROUP_CONCAT(locidx) as locs,path,name,size,GROUP_CONCAT(modtime),GROUP_CONCAT(hash_md5)
    FROM files
    GROUP BY collidx,path,name,size
  """)
  results = mergedb.execute("""
  SELECT collidx,dups,locs,COUNT(*) as nfiles,SUM(size) as totsize
  FROM dupfiles
  GROUP BY collidx,dups,locs
  """).fetchall()
  table = [
    (
      clocs[uuids[collidx]][0].collection.name, # TODO: ordering
      dups,
      locs,
      nfiles,
      totsize/(1000.0*1000.0)
    )
    for collidx,dups,locs,nfiles,totsize in results
  ]
  headers = ["Collection","# Copies","Locations","# Files","Total Size"]
  print tabulate.tabulate(table, headers=headers)

  if 0:
    for row in mergedb.execute("SELECT dups,collidx,locs,path,name,size FROM dupfiles WHERE dups=1 ORDER BY collidx,locs,path,name"):
      if isIncluded(row[3]) and isIncluded(row[4]):
        print row
