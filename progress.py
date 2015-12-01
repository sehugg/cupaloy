#!/usr/bin/python

import sys,time

class ProgressTracker:

  def __init__(self):
    self.files_visited = 0
    self.dirs_visited = 0
    self.files_max = 0
    self.dirs_max = 0
    self.count = 0
    self.last_refresh_time = 0
    self.refresh_interval = 0
    self.current_name = ''
    self.stack = []

  def push(self, name, nfiles, ndirs):
    self.stack.append((name, nfiles, ndirs, self.count))
    self.count = 0
    if nfiles:
      self.files_max += nfiles
    if ndirs:
      self.dirs_max += ndirs
    self.current_name = name
    self.refresh()
  
  def pop(self):
    name, nfiles, ndirs, count = self.stack.pop()
    if nfiles:
      self.files_max += self.count - nfiles
    self.count = count
    self.dirs_visited += 1
    self.current_name = name
  
  def inc(self, name, dsize=None):
    self.count += 1
    self.files_visited += 1
    self.current_name = name
    self.refresh()

  def refresh(self, force=False):
    t = time.time()
    if force or t - self.last_refresh_time >= self.refresh_interval:
      self.output()
      self.last_refresh_time = t
  
  def output(self):
    sys.stdout.write(str(self))
    sys.stdout.write('\r')
    sys.stdout.flush()
    
  def __repr__(self):
    if len(self.stack):
      # TODO: unicode escape?
      n = ('%s' % [self.current_name])[0:60]
      return "(%d/%d) (%d/%d) %80s" % (self.files_visited, self.files_max, self.dirs_visited, self.dirs_max, n)
    else:
      return "-"


###

if __name__ == '__main__':
  pt = ProgressTracker()
  pt.push('Foo', 10, 3)
  pt.pop()
