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

  # TODO: what if no findmnt?

###

class OSXMountInfo:

  # TODO: CoreStorage
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
          disk_uuid = part.get('DiskUUID')
          disk_name = part.get('Content')
          vol_uuid = part.get('VolumeUUID')
          vol_name = part.get('VolumeName')
          volume = MountedVolume(disk_uuid, disk_name, 'unknown', vol_uuid, vol_name, 'unknown', mp)
          mounts.append(volume)
    self.mounts = mounts

  def getVolumeAt(self, path):
    best = None
    for m in self.mounts:
      if path.startswith(m.mount_point):
        if not best or len(m.mount_point) > len(best.mount_point):
          best = m
    return best

  def getVolumeByUUID(self, uuid):
    for m in self.mounts:
      if m.vol_uuid == uuid: # TODO: ignore case?
        return m
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
  print mountInfo.getVolumeByUUID("F97D0277-5B42-3FDB-AECC-0FFDE220EC6A")
  
