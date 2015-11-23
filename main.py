#!/usr/bin/python

import getopt,sys,importlib

verbose = 0

COMMANDS = ['init','scan','status']

commandsToRun = []
arguments = []
keywords = {}

try:
  opts, args = getopt.getopt(sys.argv[1:],"vn:",["name="])
  for opt, arg in opts:
    if opt == '-v':
      verbose += 1
    elif opt in ('-n','--name'):
      keywords['name'] = arg
  for arg in args:
    if arg in COMMANDS:
      commandsToRun.append(arg)
    else:
      arguments.append(arg)
except getopt.GetoptError:
  print sys.exc_info()[1]

if len(commandsToRun) == 0:
  print 'main.py --name <name> init [directory]'
  print 'main.py [-v] scan [directories...]'
  sys.exit(2)

for cmd in commandsToRun:
  importlib.import_module(cmd).run(arguments, keywords)

sys.exit(0)
