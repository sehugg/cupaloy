#!/usr/bin/python

import sys,time

class ProgressTracker:

  def __init__(self):
    self.files_visited = 0
    self.size_visited = 0
    self.files_max = 0
    self.size_max = 0
    self.count = 0
    self.last_refresh_time = 0
    self.refresh_interval = 0
    self.current_name = ''
    self.goals = []
    
  def pushGoal(self, dfiles, dsize=0):
    self.files_max += dfiles
    self.size_max += dsize
    self.goals.append((self.files_visited+dfiles, self.size_visited+dsize))
  
  def popGoal(self):
    nf,ns = self.goals.pop()
    self.files_visited = nf
    self.size_visited = ns
    self.refresh()
  
  def inc(self, name, dsize=0):
    self.files_visited += 1
    self.size_visited += dsize
    self.files_max = max(self.files_max, self.files_visited)
    self.size_max = max(self.size_max, self.size_visited)
    self.current_name = name
    self.refresh()
    
  def incGoal(self, name, dsize=0):
    self.files_max += 1
    self.size_max += dsize
    self.inc(name, dsize)

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
    # TODO: unicode escape?
    n = ('%s' % [self.current_name])[0:60]
    if len(self.goals) == 0:
      s = "(%d)" % (self.files_visited)
    elif self.size_max > 0:
      pct = self.size_visited*100.0/self.size_max
      s = "(%d/%d) %5.1f%%" % (self.files_visited, self.files_max, pct)
    else:
      s = "(%d/%d)" % (self.files_visited, self.files_max)
    return ("%s %" + str(80-len(s)) + "s") % (s, n)


###

if __name__ == '__main__':
  pt = ProgressTracker()
  pt.push('Foo', 10, 3)
  pt.pop()
