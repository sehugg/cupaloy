#!/usr/bin/python

import unittest,tempfile,shutil,os.path
import uuid
from common import *
from main import runCommand

def setup():
  tmpdir = tempfile.mkdtemp()
  setHomeMetaDir(tmpdir)
  return tmpdir
def cleanup(tmpdir):
  shutil.rmtree(tmpdir)


class TestScan(unittest.TestCase):

  def test_args(self):
    assert runCommand(['scan']) > 0

  def test_empty(self):
    tmpdir = setup()
    assert runCommand(['scan','./tests/empty']) > 0
    cleanup(tmpdir)

  def test_scan(self):
    tmpdir = setup()
    assert runCommand(['scan','./tests/files']) == 0
    assert runCommand(['scan','./tests/files']) == 0
    cleanup(tmpdir)

  def test_scan_multiple(self):
    tmpdir = setup()
    assert runCommand(['scan','./tests/files','./tests/empty']) == 0
    cleanup(tmpdir)

###

if __name__ == '__main__':
  unittest.main()
