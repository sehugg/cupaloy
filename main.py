#!/usr/bin/python

import getopt,sys,importlib,uuid

verbose = 0

COMMANDS = ['init','scan','status','list','dups','rename']

commandsToRun = []
arguments = []
keywords = {}

try:
  opts, args = getopt.getopt(sys.argv[1:],"vn:h:fou",["verbose","name=","host=","force=","omithash","uuid="])
  for opt, arg in opts:
    if opt in ('-v','--verbose'):
      verbose += 1
    elif opt in ('-n','--name'):
      keywords['name'] = arg
    elif opt in ('-h','--host'):
      keywords['host'] = arg
    elif opt in ('-f','--force'):
      keywords['force'] = 1
    elif opt in ('-o','--omithash'):
      keywords['nohash'] = 1
    elif opt in ('-u','--uuid'):
      keywords['uuid'] = str(uuid.UUID(arg))
  for arg in args:
    if arg in COMMANDS:
      commandsToRun.append(arg)
    else:
      arguments.append(arg)
except getopt.GetoptError:
  print sys.exc_info()[1]

if len(commandsToRun) == 0:
  print """
main.py
  --name <name> init [directory]
  scan [collection...]
  [--host <name>] list
  status
  dups
  [--name <name>] [--uuid <uuid>] rename [collection]
"""
  sys.exit(2)

for cmd in commandsToRun:
  importlib.import_module(cmd).run(arguments, keywords)

sys.exit(0)
