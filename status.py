#!/usr/bin/python

import os,os.path,sys,uuid
import tabulate
from common import *

def run(args, keywords):
  globaldb = openGlobalDatabase(getGlobalDatabasePath())
  globaldb.row_factory = sqlite3.Row
  results = globaldb.execute("""
  SELECT name,
    datetime(MAX(start_time),'unixepoch'),
    total_real_size/1024.0/1024.0,
    num_real_files,
    datetime(min_mtime,'unixepoch'),
    datetime(max_mtime,'unixepoch')
  FROM scans
  GROUP BY uuid
  """)
  headers = ["Collection","Last Scan","Total MB","# Files","Oldest","Newest"]
  print tabulate.tabulate(results.fetchall(), headers=headers)
