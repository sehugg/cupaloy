#!/usr/bin/python

import sys, os, fcntl, struct
import platform
import socket

def getNodeName():
  """
  >>> type(getNodeName())
  <type 'str'>
  """
  name = platform.node()
  if not name:
    name = socket.gethostname()
    if not name:
      name = os.environ.get('COMPUTERNAME')
  assert(name)
  return name

###

if __name__ == '__main__':
  import doctest, volume
  doctest.testmod(volume)
