#!/usr/bin/python

import subprocess
from sys import platform as _platform
from plistlib import readPlistFromString

###

class LinuxMountInfo:

  def forPath(self, path):
    # TODO: what if no findmnt?
    return (
      subprocess.check_output(["findmnt","-no","uuid","-T",path]).strip(),
      subprocess.check_output(["findmnt","-no","target","-T",path]).strip()
    )

  def locationForUUID(self, uuid):
    return subprocess.check_output(["findmnt","-no","target","UUID="+uuid]).strip()

###

class OSXMountInfo:

  def __init__(self):
    xml = subprocess.check_output(["diskutil","list","-plist"])
    plist = readPlistFromString(xml)
    mounts = []
    for disk in plist['AllDisksAndPartitions']:
      partitions = disk.get('Partitions')
      if partitions:
        for part in partitions:
          mp = part.get('MountPoint')
          if mp:
            # TODO: UUID not same on Linux/OSX
            mounts.append((part.get('VolumeUUID'), mp))
    self.mounts = mounts

  def forPath(self, path):
    best = None
    for m in self.mounts:
      if path.startswith(m[1]):
        if not best or len(m[1]) > len(best[1]):
          best = m
    return best

  def locationForUUID(self, uuid):
    for m in self.mounts:
      if m[0] == uuid:
        return m[1]
    return None

# TODO: removable/online/writeonce

###

if _platform == "darwin":
  mountInfo = OSXMountInfo()
elif _platform == "win32":
  mountInfo = WindowsMountInfo()
else:
  mountInfo = LinuxMountInfo()

###

if __name__ == '__main__':
  print mountInfo.forPath("/")
  print mountInfo.forPath("a06997a7-9a7a-4395-9aa2-8630f3eb13b2")
  print mountInfo.locationForUUID("2012-07-03-20-55-41-00")
  