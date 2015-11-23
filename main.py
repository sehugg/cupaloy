#!/usr/bin/python

import getopt,sys,importlib

verbose = 0

COMMANDS = ['scan']

commandsToRun = []
arguments = []

try:
  opts, args = getopt.getopt(sys.argv[1:],"v",[])
  for opt, arg in opts:
    if opt == '-v':
      verbose += 1
  for arg in args:
    if arg in COMMANDS:
      commandsToRun.append(arg)
    else:
      arguments.append(arg)
except getopt.GetoptError:
  print 'main.py [-v] scan [directories...]'
  sys.exit(2)

if len(commandsToRun) == 0:
  print 'No commands specified.'
  sys.exit(2)

for cmd in commandsToRun:
  importlib.import_module(cmd).run(arguments)

sys.exit(0)
