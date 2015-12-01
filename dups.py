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

  clocs = getAllCollectionLocations(args)
  mergedb = getMergedFileDatabase(clocs, include_virtual=('archives' in keywords))
  uuids = clocs.keys()
  if len(uuids)==0:
    print "No collections specified."
    return False
    
  mergedb.execute("""
  CREATE TABLE dupfiles AS
    SELECT 
      COUNT(*) as dups,
      GROUP_CONCAT(DISTINCT collidx) as colls,
      GROUP_CONCAT(DISTINCT case when is_real then locidx else '['||locidx||']' end) as locs,
      path,
      name,
      size,
      SUM(is_real) as nreal
    FROM files
    WHERE size > 0 AND hash1 IS NOT NULL
    GROUP BY hash1,hash2
  """)
  results = mergedb.execute("""
  SELECT
    colls,locs,dups,
    COUNT(*) as nfiles,
    SUM(size) as totsize,
    name
  FROM dupfiles
  --WHERE INSTR(locs,',')>0
  GROUP BY colls,locs,dups
  HAVING dups>1 and MAX(size)>0
  ORDER BY colls,locs,totsize
  """).fetchall()
  table = [
    (
      colls,
      locs,
      dups,
      nfiles,
      '%10.2f' % (totsize/(1000.0*1000.0)),
      name
    )
    for colls,locs,dups,nfiles,totsize,name in results
  ]
  headers = ["Colls","Locs","# Dups","# Files","Size MB","Sample Filename"]
  print
  print tabulate.tabulate(table, headers=headers)
  print

  if 0:
    for row in mergedb.execute("SELECT dups,collidx,locs,path,name,minsize,maxsize,mintime,maxtime FROM dupfiles WHERE dups=1 ORDER BY collidx,locs,path,name"):
      if isIncluded(row[3]) and isIncluded(row[4]):
        print row
  if 0:
    for row in mergedb.execute("SELECT dups,collidx,locs,path,name,minsize,maxsize,mintime,maxtime FROM dupfiles WHERE mintime!=maxtime ORDER BY collidx,locs,path,name"):
      if isIncluded(row[3]) and isIncluded(row[4]):
        print row
