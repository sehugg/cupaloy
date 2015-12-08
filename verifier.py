#!/usr/bin/python

import subprocess,os.path,sys
from common import verbose

VERIFIERS=[
  {
    'mimetype':	'image/jpeg',
    'exts':	['.jpg','.jpe','.jpeg'],
    'cmdline':	'jpeginfo -i -c $PATH',
  },
  {
    'mimetype': 'video/mpeg',
    'exts':	['.avi','.mpeg','.mpg','.mpe','.mpv','.mp4','.m4v','.m4p','.m2v',
                 '.mov','.qt','.iso','.webm',
                 '.mkv','.mk3d','.mka','.mks',
                 '.vob','.ogv','.ogg','.drc','.wmv','.yuv','.rm','.rmvb','.asf',
                 '.svi','.3gp','.3g2','.mxf','.roq','.nsv',
                 '.flv','.f4v','.f4p','.f4a','.f4b'],
    'cmdline':	'ffmpeg -v error -i $PATH -f null -',
  }
]

###

def verifyFile(scanfile):
  """
  >>> from common import ScanFile
  >>> f = ScanFile("tests/files/verifier/video1.mp4",0,0)
  >>> f.path = f.key
  >>> a,b = verifyFile(f)
  >>> a['mimetype']
  'video/mpeg'
  >>> b
  """
  # TODO: support file handles too
  if not hasattr(scanfile,'path'):
    return (None,None)
  fbase,fext = os.path.splitext(scanfile.path)
  fext = fext.lower()
  besterr = None
  bestfmt = None
  for v in VERIFIERS:
    exts = v.get('exts')
    if exts and fext in exts:
      args = v.get('cmdline').split()
      for i in range(0,len(args)):
        if args[i] == '$PATH':
          args[i] = scanfile.path
      try:
        output = subprocess.check_output(args)
        if verbose:
          print scanfile,args,output
        return (v,None)
      except subprocess.CalledProcessError:
        print args[0],scanfile
        besterr = sys.exc_info()[1]
  return (bestfmt,besterr)

###

if __name__ == '__main__':
  import doctest, verifier
  doctest.testmod(verifier)
