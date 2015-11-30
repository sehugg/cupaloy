#!/usr/bin/python

import os,os.path,sys,uuid
import tabulate
from common import *

def run(args, keywords):
  globaldb = openGlobalDatabase(getGlobalDatabasePath(host=keywords.get('host')))
  globaldb.row_factory = sqlite3.Row
  results = globaldb.execute("""
  SELECT 
    name,
    datetime(MAX(start_time),'unixepoch'),
    ROUND(total_real_size/1000.0/1000.0,1),
    num_real_files,
    datetime(min_mtime,'unixepoch'),
    datetime(max_mtime,'unixepoch')
  FROM scans
  GROUP BY uuid
  """)
  headers = ["Collection","Last Scan","Total MB","# Files","Oldest","Newest"]
  print
  print tabulate.tabulate(results.fetchall(), headers=headers)
  print
