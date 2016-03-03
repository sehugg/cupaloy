#!/usr/bin/python

import subprocess,os.path,sys
from common import verbose

VERIFIERS=[
  {
    'exts':	['.pdf'],
    'cmdline':	'pdfinfo $PATH',
    'cmdver':	'pdfinfo -v',
  },
  {
    'exts':	['.jpg','.jpe','.jpeg','.png','.gif','.ico','.bmp',
                 '.pnm','.ppm','.pgm','.pfm',
                 '.tif','.tiff','.tga',
                 '.pict','.pcx',
                 '.svg',
                 ],
    'cmdline':	'identify $PATH',
    'cmdver':	'identify -version',
  },
  {
    'exts':	['.jpg','.jpe','.jpeg'],
    'cmdline':	'jpeginfo -i -c $PATH',
    'cmdver':	'jpeginfo --version',
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
    'cmdver':	'ffmpeg -version',
  }
]

# see which programs we have (TODO: get version #)

for v in VERIFIERS:
  cmdline = v.get('cmdver') or v.get('cmdline').split()[0]
  args = cmdline.split()
  try:
    output = subprocess.check_output(args)
    v['enabled'] = True
    print args,'ENABLED'
  except:
    print args,sys.exc_info()

###

def verifyFile(scanfile):
  """
  >>> from common import ScanFile
  >>> f = ScanFile("tests/files/verifier/video1.mp4",0,0)
  >>> f.path = f.key
  >>> a,b = verifyFile(f)
  >>> a['cmdver']
  'ffmpeg -version'
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
    if v.get('enabled') and exts and fext in exts:
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
