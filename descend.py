#!/usr/bin/python
#
# Creates rows out of recursive data structures.
#
# Intended to prep data for use by Hive/Impala.

import sys
import os
import tempfile
import zipfile
import json
import gzip
import shutil
import logging

LOG = logging.getLogger(__name__)

def descend_path(stack, cur, name=None):
  if name is None:
    name = cur
  if os.path.isfile(cur):
    yield stack + ["file", cur]
    if cur.endswith(".zip"):
      tmpdir = tempfile.mkdtemp()
      zipfile.ZipFile(cur).extractall(tmpdir)
      for x in descend_path(stack + ["file", name], tmpdir, cur):
        yield x
      shutil.rmtree(tmpdir)
    elif cur.endswith(".gz"):
      for x in descend_file(stack + ["file", name], cur[:-3], gzip.GzipFile(cur)):
        yield x
    else:
      f = file(cur)
      for x in descend_file(stack + ["file", name], cur, f):
        yield x
      f.close()
  elif os.path.isdir(cur):
    for f in os.listdir(cur):
      for x in descend_path(stack, os.path.join(cur, f), name + "/" + f):
        yield x

def descend_file(stack, filename, cur):
  if filename.endswith(".json"):
    data = json.load(cur)
    for x in descend_obj(stack, data):
      yield x
  else:
    for i, x in enumerate(cur.readlines()):
      yield stack + ["line", i] + ["content", x.strip()]

def descend_obj(stack, cur):
  if isinstance(cur, list):
    for i, v in enumerate(cur):
      for x in descend_obj(stack + ["array", i], v):
        yield x
  elif isinstance(cur, dict):
    for k, v in cur.iteritems():
      for x in descend_obj(stack + ["map", k], v):
        yield x
  else:
    yield stack + ["val", cur]

DEV_NULL = file("/dev/null", "w")
      
def main(cur):
  for x in descend_path([], cur):
    try:
      sys.stdout.write(str(len(x)))
      sys.stdout.write("\001")
      for z in x:
        sys.stdout.write(str(z))
        sys.stdout.write("\001")
      sys.stdout.write("\n")
    except Exception, ex:
      # Pretty much a mystery to me why safe_stringify doesn't work here.
      LOG.exception("Skipping row..." + repr(x))

def safe_stringify(x):
  try:
    return unicode(x)
  except Exception, e:
    return "__ERR__" + repr(x)

if __name__ == "__main__":
  logging.basicConfig(level=logging.INFO)
  main(sys.argv[1])
