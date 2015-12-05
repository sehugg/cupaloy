#!/usr/bin/python

import subprocess,re
from sys import platform as _platform
from plistlib import readPlistFromString

###

class MountedVolume:

  def __init__(self, disk_uuid, disk_label, mediatype, vol_uuid, vol_label, fstype, mount_point):
    self.disk_uuid = disk_uuid
    self.disk_label = disk_label
    self.mediatype = mediatype
    self.vol_uuid = vol_uuid
    self.vol_label = vol_label
    self.fstype = fstype
    self.mount_point = mount_point
    
  def __repr__(self):
    return "%s:%s:%s:%s:%s:%s:%s" % (self.disk_uuid, self.disk_label, self.mediatype, self.vol_uuid, self.vol_label, self.fstype, self.mount_point)

###

class LinuxMountInfo:

  def findmnt(self, extra_args):
    args = ["findmnt","-Po","target,fstype,label,uuid,partlabel,partuuid"]
    args.extend(extra_args)
    output = subprocess.check_output(args).strip()
    matches = re.findall('(\\w+)="(.*?)"', output)
    dict = {}
    for k,v in matches:
      dict[k] = v
    mediatype = 'unknown'
    volume = MountedVolume(dict['UUID'], dict['LABEL'], mediatype, dict['PARTUUID'], dict['PARTLABEL'], dict['FSTYPE'], dict['TARGET'])
    return volume
  
  def getVolumeAt(self, path):
    return self.findmnt(['-T',path])
    
  def getVolumeByUUID(self, uuid):
    return self.findmnt(['UUID='+uuid])

  def forPath(self, path):
    volume = self.getVolumeAt(path)
    return (volume.disk_uuid, volume.mount_point)
    # TODO: what if no findmnt?

  def locationForUUID(self, uuid):
    volume = self.getVolumeByUUID(uuid)
    return volume.mount_point

###

class OSXMountInfo:

  def __init__(self):
    xml = subprocess.check_output(["diskutil","list","-plist"])
    plist = readPlistFromString(xml)
    mounts = []
    for disk in plist['AllDisksAndPartitions']:
      list = [disk]
      partitions = disk.get('Partitions')
      if partitions:
        list.extend(partitions)
      for part in list:
        mp = part.get('MountPoint')
        if mp:
          # TODO: UUID not same on Linux/OSX
          vol_uuid = part.get('VolumeUUID')
          if vol_uuid:
            mounts.append((vol_uuid, mp))
          else:
            mounts.append((part['VolumeName'], mp)) # TODO: no UUID
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

###

class WindowsMountInfo:

  def __init__(self):
    raise Exception("Not yet implemented")
    # TODO

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
  print mountInfo.getVolumeAt("/")
  print mountInfo.getVolumeAt("/media/huggvey/ISOIMAGE/boot")
  print mountInfo.getVolumeAt("/Volumes/Passport/hdbackup")
  print mountInfo.getVolumeAt("a06997a7-9a7a-4395-9aa2-8630f3eb13b2")
  print mountInfo.getVolumeByUUID("2012-07-03-20-55-41-00")
