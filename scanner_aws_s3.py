#!/usr/bin/python

import os.path,urlparse
import boto3
from common import *
from contextlib import closing

class AWSS3File(ScanFile):

  def __init__(self, key, size, mtime, s3obj):
    ScanFile.__init__(self, key, size, mtime)
    self.s3obj = s3obj

  def getFileHandle(self):
    if self.s3obj.storage_class == 'GLACIER':
      return None
    else:
      return closing(self.s3obj.get(IfMatch=self.s3obj.e_tag)['Body'])
  

class AWSS3Scanner:

  def __init__(self, url):
    pr = urlparse.urlparse(url)
    assert pr.scheme == 's3' 
    print "Connecting to S3..."
    self.s3conn = boto3.resource('s3')
    self.s3 = boto3.client('s3')
    print "Using bucket '%s'" % pr.netloc
    self.bucket = self.s3conn.Bucket(pr.netloc)

  def scan(self):
    for obj in self.bucket.objects.all():
      dir,filename = os.path.split(obj.key)
      if isIncluded(dir) and isIncluded(filename): # TODO, just one call?
        e_tag = obj.e_tag
        last_modified = fixTimestamp(obj.last_modified)
        size = obj.size
        storage_class = obj.storage_class
        scanfile = AWSS3File(obj.key, size, last_modified, obj)
        yield scanfile

if __name__ == '__main__':
  for x in AWSS3Scanner("s3://assets.puzzlingplans.com").scan():
    print x
    with x.getFileHandle() as f:
      print len(f.read())
      
