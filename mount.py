#!/usr/bin/python

import subprocess,re
from sys import platform as _platform
from plistlib import readPlistFromString

###

class MountedVolume:

  def __init__(self, disk_uuid, disk_label, mediatype, vol_uuid, vol_label, fstype, mount_point):
    assert disk_uuid
    assert vol_uuid
    self.disk_uuid = disk_uuid.lower()
    self.disk_label = disk_label
    self.mediatype = mediatype
    self.vol_uuid = vol_uuid.lower()
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
    try:
      output = subprocess.check_output(args).strip()
    except subprocess.CalledProcessError:
      return None
    matches = re.findall('(\\w+)="(.*?)"', output)
    dict = {}
    for k,v in matches:
      dict[k] = v
    mediatype = 'unknown'
    volume = MountedVolume(dict['UUID'], dict['LABEL'], mediatype, dict['PARTUUID'], dict['PARTLABEL'], dict['FSTYPE'], dict['TARGET'])
    return volume
  
  def getVolumeAt(self, path):
    assert path
    return self.findmnt(['-T',path])
    
  def getVolumeByUUID(self, uuid):
    assert uuid
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
          mounts.append(mp.lower())
    self.mounts = mounts
    self.uuid_case_map = {}

  def volumeFor(self, path):
    assert path
    try:
      xml = subprocess.check_output(["diskutil","info","-plist",path])
    except subprocess.CalledProcessError:
      return None
    plist = readPlistFromString(xml)
    disk_uuid = plist.get('DiskUUID')
    disk_name = plist.get('Content')
    vol_uuid = plist.get('VolumeUUID')
    vol_name = plist.get('VolumeName')
    writable = plist.get('Writable')
    fstype = plist.get('FilesystemType')
    mediatype = plist.get('MediaType')
    mount_point = plist.get('MountPoint')
    # TODO: NTFS has no UUID
    if not disk_uuid:
      disk_uuid = plist.get('VolumeName')
    if not vol_uuid:
      vol_uuid = plist.get('VolumeName')
    # because diskutil is case-sensitive when looking up volume names...
    self.uuid_case_map[vol_uuid.lower()] = vol_uuid
    self.uuid_case_map[disk_uuid.lower()] = disk_uuid
    return MountedVolume(disk_uuid, disk_name, mediatype, vol_uuid, vol_name, fstype, mount_point)

  def getVolumeAt(self, path):
    assert path
    path = path.lower()
    best = None
    for m in self.mounts:
      if path.startswith(m):
        if not best or len(m) > len(best):
          best = m
    return self.volumeFor(best) if best else None

  def getVolumeByUUID(self, uuid):
    assert uuid
    uuid2 = self.uuid_case_map.get(uuid.lower())
    if uuid2:
      uuid = uuid2
    return self.volumeFor(uuid)

###

class WindowsMountInfo:

  def __init__(self):
    raise Exception("Not yet implemented")
    # TODO

# TODO: removable/online/writeonce
# TODO: case insensitive UUIDs?

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
  print mountInfo.getVolumeAt("/Volumes/My Passport")
  print mountInfo.getVolumeAt("/Volumes/my passport")
  print mountInfo.getVolumeByUUID("My Passport")
  print mountInfo.getVolumeByUUID("my passport")
  print mountInfo.getVolumeAt("a06997a7-9a7a-4395-9aa2-8630f3eb13b2")
  print mountInfo.getVolumeByUUID("2012-07-03-20-55-41-00")
  print mountInfo.getVolumeByUUID("F97D0277-5B42-3FDB-AECC-0FFDE220EC6A")

