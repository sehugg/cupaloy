#!/usr/bin/python

import unittest,tempfile,shutil,os.path
import uuid
from common import *
from main import runCommand

class TestInit(unittest.TestCase):

  def test_args(self):
    assert runCommand(['scan']) > 0

  def test_empty(self):
    assert runCommand(['scan','./tests/empty']) > 0

  def test_scan(self):
    tmpdir = tempfile.mkdtemp()
    setHomeMetaDir(tmpdir)
    assert runCommand(['scan','./tests/files']) == 0
    assert runCommand(['scan','./tests/files']) == 0
    shutil.rmtree(tmpdir)

###

if __name__ == '__main__':
  unittest.main()
