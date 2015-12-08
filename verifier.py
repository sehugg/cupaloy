#!/usr/bin/python

import subprocess,os.path,sys
from common import verbose

VERIFIERS=[
  {
    'mimetype':	'image/jpeg',
    'exts':	['.jpg','.jpe','.jpeg'],
    'cmdline':	['jpeginfo','-i','-c'],
  }
]

###

def verifyFile(scanfile):
  # TODO: support file handles too
  if not hasattr(scanfile,'path'):
    return None
  fbase,fext = os.path.splitext(scanfile.path)
  fext = fext.lower()
  for v in VERIFIERS:
    exts = v.get('exts')
    if exts and fext in exts:
      args = v.get('cmdline')[:]
      args.append(scanfile.path)
      try:
        output = subprocess.check_output(args)
        if verbose:
          print scanfile,args,output
        return v
      except subprocess.CalledProcessError:
        print scanfile,sys.exc_info()
  return None
