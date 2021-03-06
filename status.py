#!/usr/bin/python

import os,os.path,sys,uuid,sqlite3,math,urlparse,string
import tabulate
from common import *

find_hashes = 0 # TODO

scan_interval_1 = 86400*2
scan_interval_2 = 86400*30

def pct(n,d,dups=None):
  if dups==1:
    return ''
  elif n==None or not d:
    return '-'
  elif n==d:
    return '100%'
  else:
    return "%0.1f%%" % (math.floor(n*1000.0/d)/10.0)

def run(args, keywords):
  
  find_hashes = len(args) > 0 # TODO: for now we disable hash-finding if getting status on all collections
  clocs = getAllCollectionLocations(args)
  mergedb = getMergedFileDatabase(clocs, include_virtual=('archives' in keywords))
  addMergedVolumeDatabase(mergedb)
  uuids = clocs.keys()
  if len(uuids)==0:
    print "No collections specified."
    return False

  mergedb.execute("""
  CREATE TABLE dupfiles AS
    SELECT
      COUNT(*) as dups,
      collidx,
      GROUP_CONCAT(case when is_real then locidx else '['||locidx||']' end) as locs,
      path,name,
      MIN(IFNULL(size,-1)) as minsize,
      MAX(IFNULL(size,0)) as maxsize,
      MIN(IFNULL(modtime,-1)) as mintime,
      MAX(IFNULL(modtime,0)) as maxtime,
      MIN(IFNULL(hash1,'-')) as minhash,
      MAX(IFNULL(hash1,'_')) as maxhash,
      SUM(has_errors) as nerrors
    FROM files
    WHERE size > 0
    GROUP BY collidx,path,name
  """)

  if find_hashes:
    mergedb.execute("""
    CREATE TABLE duphashes AS
      SELECT 
        COUNT(*) as dups,
        collidx,
        GROUP_CONCAT(DISTINCT case when is_real then locidx else '['||locidx||']' end) as locs,
        size
      FROM files
      WHERE size > 0 AND hash1 IS NOT NULL
      GROUP BY collidx,hash1,hash2
    """)

  mergedb.execute("""
  CREATE TABLE stats AS
    SELECT
      collidx,dups,locs,
      COUNT(*) as nfiles,
      SUM(1.0*maxsize) as totsize,
      SUM(minsize=maxsize) as samesize,
      SUM(mintime=maxtime) as sametime,
      SUM(minhash=maxhash) as samehash,
      SUM(nerrors) as nerrors,
      %s as hashsize
    FROM dupfiles df
    GROUP BY collidx,dups,locs
  """ % ("""
      (SELECT SUM(1.0*size) FROM duphashes dh
        WHERE dh.collidx=df.collidx
          AND dh.dups=df.dups
          AND dh.locs=df.locs)
  """ if find_hashes else "0"))

  results = mergedb.execute("""
  SELECT s.*, t.ncollfiles, t.totcollsize, t.tothashsize
  FROM stats s
  JOIN (
    SELECT
      collidx,
      SUM(nfiles) as ncollfiles,
      SUM(totsize) as totcollsize,
      SUM(hashsize) as tothashsize
    FROM stats 
    GROUP BY collidx
    ) t ON s.collidx=t.collidx
  ORDER BY collidx,hashsize,totsize,locs
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
      pct(nerrors,nfiles,dups),
      pct(hashsize,tothashsize) if find_hashes else '',
    )
    for collidx,dups,locs,nfiles,totsize,samesize,sametime,samehash,nerrors,hashsize,ncollfiles,totcollsize,tothashsize in results
  ]
  headers = ["Collection","# Copies","Locations","# Files","Total MB","Coverage","=Size","=Hash","=Time","Errors",
    "Hash Match" if find_hashes else '']
  print
  print tabulate.tabulate(table, headers=headers)
  print
  
  if 1:
    # TODO: what if --archives?
    table = []
    for uuid,cloclist in clocs.items():
      collidx = uuids.index(uuid)
      rows = [row for row in results if row[0] == collidx]
      if len(rows)==0:
        continue
      last = rows[-1]
      collidx,dups,locs,nfiles,totsize,samesize,sametime,samehash,nerrors,hashsize,ncollfiles,totcollsize,tothashsize = last
      last2 = rows[-2] if len(rows)>1 else None
      if last2:
        collidx2,dups2,locs2,nfiles2,totsize2,samesize2,sametime2,samehash2,nerrors2,hashsize2,ncollfiles2,totcollsize2,tothashsize2 = last2
      collection = cloclist[0].collection
      netlocs = [string.join(urlparse.urlparse(cloc.url)[0:2],'://') for cloc in cloclist]
      netlocset = set(netlocs)
      unique_locs = len(netlocset)
      scanages = [sessionStartTime - fixTimestamp(cloc.scantime) for cloc in cloclist]
      s = ''
      r = []
      if unique_locs < 2:
        s = "Not replicated"
        r.append("add replica")
      elif totsize==totcollsize and samesize==nfiles and samehash==nfiles:
        s = "Fully replicated across %d locations" % (unique_locs)
      else:
        if find_hashes and hashsize*1.0/tothashsize > totsize*1.0/totcollsize:
          s = "%s duplicated across %d locations with %s of files moved" % (
            pct(hashsize,tothashsize), unique_locs, pct(ncollfiles-nfiles,ncollfiles))
          r.append("check directory structure")
        else:
          num = 1.0*totsize*samehash
          denom = 1.0*totcollsize*nfiles
          reppct = pct(num, denom)
          s = "%s replicated across %d locations" % (reppct, unique_locs)
          if last2 and unique_locs > 2:
            num2 = 1.0*(totsize+totsize2)*(samehash+samehash2)
            denom2 = 1.0*totcollsize*(nfiles+nfiles2)
            reppct2 = pct(num2, denom2)
            if reppct2 != reppct:
              s += ", %s across %d" % (reppct2, unique_locs-1)
      if scanages[0] > scan_interval_1:
        r.append("scan on %s" % cloclist[0].locname)
      elif scanages[-1] > scan_interval_2:
        r.append("scan on %s" % cloclist[-1].locname)
      s += '.'
      table.append((collection.name, s, string.join(r, ', ')))
    print tabulate.tabulate(table, headers=["Collection","Status","Recommendations"])

  if 'list' in keywords:
    lastpath = ''
    for row in mergedb.execute("""
      SELECT
        path,name,
        SUM(dups),
        GROUP_CONCAT(collidx),
        locs,
        AVG((minsize+maxsize)/2),
        GROUP_CONCAT(minsize)=GROUP_CONCAT(maxsize) as sizeequal,
        GROUP_CONCAT(minhash)=GROUP_CONCAT(maxhash) as hashequal
      FROM dupfiles
      GROUP BY path,name
      --HAVING (NOT sizeequal OR NOT hashequal)
      ORDER BY path,name
    """):
      if 1: #isIncluded(row[0]) and isIncluded(row[1]):
        if lastpath != row[0]:
          print row[0]
          lastpath = row[0]
        print "%40s %5d %10s %10s %10d %d %d" % row[1:]
  if 0:
    for row in mergedb.execute("""
      SELECT COUNT(*),dups,collidx,locs,path,name,SUM(minsize),SUM(maxsize)
      FROM dupfiles
      WHERE dups=1
      GROUP BY collidx,locs,path
      ORDER BY collidx,locs,path
    """):
      if isIncluded(row[4]) and isIncluded(row[5]):
        print row
  if 0:
    for row in mergedb.execute("SELECT dups,collidx,locs,path,name,minsize,maxsize,mintime,maxtime FROM dupfiles WHERE dups=1 ORDER BY collidx,locs,path,name"):
      if isIncluded(row[3]) and isIncluded(row[4]):
        print row
  if 0:
    for row in mergedb.execute("SELECT dups,collidx,locs,path,name,minsize,maxsize,mintime,maxtime FROM dupfiles WHERE mintime!=maxtime ORDER BY collidx,locs,path,name"):
      if isIncluded(row[3]) and isIncluded(row[4]):
        print row
