#!/usr/bin/python

import subprocess

def mountInfoForPath(path):
  # TODO: what if no findmnt?
  return (
    subprocess.check_output(["findmnt","-no","uuid","-T",path]).strip(),
    subprocess.check_output(["findmnt","-no","target","-T",path]).strip()
  )

def mountLocationForUUID(uuid):
  return subprocess.check_output(["findmnt","-no","target","UUID="+uuid]).strip()

###

if __name__ == '__main__':
  print mountInfoForPath("/")
  print mountInfoForPath("a06997a7-9a7a-4395-9aa2-8630f3eb13b2")
  print mountLocationForUUID("2012-07-03-20-55-41-00")
  