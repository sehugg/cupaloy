#!/usr/bin/python

import subprocess,re,sqlite3
from sys import platform as _platform
from plistlib import readPlistFromString

###

class MountedVolume:

  def __init__(self, disk_uuid, disk_label, mediatype, vol_uuid, vol_label, fstype, mount_point):
    if not vol_uuid:
      vol_uuid = disk_uuid # for Linux CDROMs
    assert disk_uuid
    assert vol_uuid
    self.disk_uuid = disk_uuid.lower()
    self.disk_label = disk_label
    self.mediatype = mediatype
    self.vol_uuid = vol_uuid.lower()
    self.vol_label = vol_label
    self.fstype = fstype
    self.mount_point = mount_point
    self.usage = None # TODO: online/offline/removable/etc
    
  def updateDatabase(db):
    db.execute("""
    REPLACE INTO volumes (vol_uuid,vol_label,disk_uuid,disk_label,mediatype,fstype,mount_point,last_seen_from,last_seen_time,usage)
    VALUES (?,?,?,?,?,?,?,?,?,?)
    """, [self.vol_uuid, self.vol_label, self.disk_uuid, self.disk_label, self.mediatype, self.fstype,
          sessionStartTime, getNodeName(), self.usage])
    
  def __repr__(self):
    return "%s:%s:%s:%s:%s:%s:%s" % (self.disk_uuid, self.disk_label, self.mediatype, self.vol_uuid, self.vol_label, self.fstype, self.mount_point)

###

class LinuxMountInfo:

  def __init__(self):
    #/dev/mapper/mint--vg-root / ext4 rw,relatime,errors=remount-ro,data=ordered 0 0
    self.uuids = {}
    self.mounts = {}
    self.uuid_case_map = {}
    with open('/proc/mounts','r') as f:
      lines = f.readlines()
      for l in lines:
        dev,target,fstype,opts,arg1,arg2 = l.split()
        if dev[0] == '/':
          args = ["udevadm","info","-q","all","-n",dev]
          output = subprocess.check_output(args).strip()
          keys = {}
          for l in output.split('\n'):
            if l[:3] == 'E: ':
              k,v = l[3:].split('=')
              keys[k] = v
          uuid = keys.get('ID_FS_UUID')
          type = keys.get('ID_FS_TYPE')
          label = keys.get('ID_FS_LABEL')
          mediatype = keys.get('ID_TYPE')
          partuuid = keys.get('ID_PART_ENTRY_UUID')
          partlabel = keys.get('ID_SERIAL')
          if not uuid:
            uuid = partuuid
          if not uuid:
            uuid = partlabel
          volume = MountedVolume(uuid, label, mediatype, partuuid, partlabel, fstype, target)
          self.mounts[target] = volume
          self.uuids[uuid] = volume
          self.uuid_case_map[uuid.lower()] = uuid
          if partuuid:
            self.uuids[partuuid] = volume
            self.uuid_case_map[partuuid.lower()] = partuuid

  def getVolumeAt(self, path):
    assert path
    path = path.lower()
    best = None
    bestvol = None
    for m,volume in self.mounts.items():
      if path.startswith(m):
        if not best or len(m) > len(best):
          best = m
          bestvol = volume
    return bestvol

  def getVolumeByUUID(self, uuid):
    assert uuid
    uuid2 = self.uuid_case_map.get(uuid.lower())
    if uuid2:
      uuid = uuid2
    return self.uuids.get(uuid)

###

class LinuxMountInfo2:

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
    volume = self.findmnt(['PARTUUID='+uuid])
    if not volume:
      volume = self.findmnt(['UUID='+uuid])
    return volume

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
      else:
        partitions = disk.get('APFSVolumes')
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
  print mountInfo.getVolumeAt("/tmp")
  print mountInfo.getVolumeAt("/media/huggvey/ISOIMAGE/boot")
  print mountInfo.getVolumeAt("/Volumes/Passport/hdbackup")
  print mountInfo.getVolumeAt("/Volumes/My Passport")
  print mountInfo.getVolumeAt("/Volumes/my passport")
  print mountInfo.getVolumeByUUID("My Passport")
  print mountInfo.getVolumeByUUID("my passport")
  print mountInfo.getVolumeByUUID("658c8dbc-f086-4d6c-9f33-180d3a008f1b")
  print mountInfo.getVolumeByUUID("2012-07-03-20-55-41-00")
  print mountInfo.getVolumeByUUID("F97D0277-5B42-3FDB-AECC-0FFDE220EC6A")
  print mountInfo.getVolumeByUUID("581d95d2-c6f4-4a2e-b92b-de81e8b44918")
  print mountInfo.getVolumeByUUID("6f305923-f6fd-4e89-bfc4-5fad55c556eb")

