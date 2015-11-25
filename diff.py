#!/usr/bin/python

import os,os.path,sys,uuid
import tabulate
from common import *

def run(args, keywords):
  db = openGlobalDatabase(getGlobalDatabasePath())
  db.execute("ATTACH DATABASE ? AS diff", [getGlobalDatabasePath(host=keywords['host'])])
  results = db.execute("""
  SELECT name,size.mtime FROM files
  EXCEPT
  SELECT name,size,mtime FROM diff.files
  """)
  headers = ["Name","Size","Mod Time"]
  print tabulate.tabulate(results.fetchall(), headers=headers)
