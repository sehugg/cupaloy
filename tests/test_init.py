#!/usr/bin/python

import unittest,tempfile,shutil,os.path
import uuid
from common import *
from main import runCommand

class TestInit(unittest.TestCase):

  def test_args(self):
    assert runCommand(['init']) > 0
    assert runCommand(['--name','Foo','init']) > 0

  def test_init(self):
    tmpdir = tempfile.mkdtemp()
    setHomeMetaDir(tmpdir)
    uid = str(uuid.uuid4())
    assert runCommand(['--name','Foo','--uuid',uid,'init',tmpdir]) == 0
    assert os.path.isdir(os.path.join(tmpdir, METADIR))
    assert os.path.isfile(os.path.join(tmpdir, METADIR, Collection.METAFILENAME))
    cloc = loadCollectionLocation(tmpdir)
    print cloc
    assert cloc
    assert cloc.collection
    assert cloc.collection.name == 'Foo'
    assert cloc.collection.uuid == uid
    assert cloc.url
    assert runCommand(['init',tmpdir]) > 0 # already exists
    shutil.rmtree(tmpdir)

###

if __name__ == '__main__':
  unittest.main()
