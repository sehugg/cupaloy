#!/usr/bin/python

import getopt,sys,importlib,uuid
import common

COMMANDS = ['init','scan','status','list','dups','rename','retire']

def runCommand(argv):

  verbose = 0
  commandsToRun = []
  arguments = []
  keywords = {}
  
  try:
    opts, args = getopt.getopt(argv,"vn:h:fourAlu:",["verbose","name=","host=","force","omithash","uuid=","rescan","archives","list","url="])
    for opt, arg in opts:
      if opt in ('-v','--verbose'):
        verbose += 1
      elif opt in ('-n','--name'):
        keywords['name'] = arg
      elif opt in ('-h','--host'):
        keywords['host'] = arg
      elif opt in ('-f','--force'):
        keywords['force'] = 1
      elif opt in ('-r','--rescan'):
        keywords['rescan'] = 1
      elif opt in ('-o','--omithash'):
        keywords['nohash'] = 1
      elif opt in ('-u','--uuid'):
        keywords['uuid'] = str(uuid.UUID(arg))
      elif opt in ('-A','--archives'):
        keywords['archives'] = 1
      elif opt in ('-l','--list'):
        keywords['list'] = 1
      elif opt in ('-U','--url'):
        keywords['url'] = arg
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
    --name <name> [--uuid uuid] init [directory]
    [--force] [--rescan] [--omithash] scan [collection...]
    [--host <name>] list
    [--archives] status [collection...]
    [--archives] dups
    [--name <name>] [--uuid <uuid>] rename [collection]
    retire [collection]
  """
    return 2

  common.verbose = verbose
  for cmd in commandsToRun:
    result = importlib.import_module(cmd).run(arguments, keywords)
    if result == False:
      return 1
  return 0

###

if __name__ == '__main__':
  sys.exit(runCommand(sys.argv[1:]))
