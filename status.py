#!/usr/bin/python

import os,os.path,sys,uuid,sqlite3,math
import tabulate
from common import *

def pct(n,d,dups=None):
  if dups==1:
    return ''
  elif n==None or not d:
    return '-'
  elif n==d:
    return '100%'
  else:
    return "%5.1f%%" % (math.floor(n*1000.0/d)/10.0)

def run(args, keywords):
  clocs = getAllCollectionLocations()
  mergedb = getMergedFileDatabase(clocs)
  uuids = clocs.keys()
  mergedb.execute("""
  CREATE TABLE dupfiles AS
    SELECT COUNT(*) as dups,collidx,GROUP_CONCAT(locidx) as locs,path,name,
      MIN(IFNULL(size,-1)) as minsize,
      MAX(IFNULL(size,0)) as maxsize,
      MIN(IFNULL(modtime,-1)) as mintime,
      MAX(IFNULL(modtime,0)) as maxtime,
      MIN(IFNULL(hash_md5,'')) as minhash,
      MAX(IFNULL(hash_md5,'-')) as maxhash,
      SUM(has_errors) as nerrors
    FROM files
    GROUP BY collidx,path,name
  """)
  mergedb.execute("""
  CREATE TABLE stats AS
    SELECT
      collidx,dups,locs,
      COUNT(*) as nfiles,
      SUM(maxsize) as totsize,
      SUM(minsize=maxsize) as samesize,
      SUM(mintime=maxtime) as sametime,
      SUM(minhash=maxhash) as samehash,
      SUM(nerrors) as nerrors
    FROM dupfiles
    GROUP BY collidx,dups,locs
  """)
  results = mergedb.execute("""
  SELECT s.*, t.ncollfiles, t.totcollsize
  FROM stats s
  JOIN (SELECT collidx,COUNT(*) as ncollfiles,SUM(totsize) as totcollsize FROM stats GROUP BY collidx) t ON s.collidx=t.collidx
  ORDER BY collidx,dups,locs
  """).fetchall()
  table = [
    (
      clocs[uuids[collidx]][0].collection.name, # TODO: ordering
      dups,
      locs,
      nfiles,
      '%10.2f' % (totsize/(1000.0*1000.0)),
      pct(totsize,totcollsize),
      pct(samesize,nfiles,dups),
      pct(samehash,nfiles,dups),
      pct(sametime,nfiles,dups),
      pct(nerrors,nfiles,dups)
    )
    for collidx,dups,locs,nfiles,totsize,samesize,sametime,samehash,nerrors,ncollfiles,totcollsize in results
  ]
  headers = ["Collection","# Copies","Locations","# Files","Total MB","% Covered","=Size","=Hash","=Time","Errors"]
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
