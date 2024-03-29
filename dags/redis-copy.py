#!/usr/bin/env python

import redis
import os
import logging
from tempfile import NamedTemporaryFile
import subprocess

logging.basicConfig(level=logging.DEBUG)

SOURCE_DIR='/srv/cryoem/temalpha/'

REDIS_KEY='TEMALPHA'
DRY_RUN=False #True
CHMOD=' --chmod=ug+x,u+rw,g+r,g-w,o-rwx '
PARALLEL=5

HOST = os.environ['REDIS_SERVICE_HOST']
PORT = os.environ['REDIS_SERVICE_PORT']

BATCH = 25 

DB = 4
if 'REDIS_RSYNC_DB' in os.environ:
  DB = os.environ['REDIS_RSYNC_DB']

PASSWORD = None
if 'REDIS_CONFIG' in os.environ:
  PASSWORD = os.environ['REDIS_CONFIG'].split().pop(-1)


os.chdir( SOURCE_DIR )
if not os.path.exists( '.online' ):
  raise Exception(f"File system {SOURCE_DIR} not mounted!")

logging.debug( f'connecting to redis server {HOST}:{PORT}/{DB}' ) # with {PASSWORD}' )
client = redis.StrictRedis( host=HOST, port=PORT, db=DB, password=PASSWORD )

queued = client.llen( REDIS_KEY )
logging.info('Queued files for transfer: %s' % (queued,)) 

def get_files( client, batch_size=BATCH, exclude=['.xml',] ):
  transfer = {}
  for i in range( BATCH ):
    ret = client.blpop( REDIS_KEY, timeout=1 )
    if ret == None:
      break
    try:
      _, data = ret
      #logging.info("DATA: %s" % (data,))
      s, t = data.decode("utf-8").split( ' -> ' )
      if not t in transfer:
        transfer[t] = []
      add = True
      for f in exclude:
        if f in s:
          add = False
          break
      if add:
        transfer[t].append( s )
    except Exception as e:
      logging.warn("Error: %s" % (e,))
      pass
  return transfer


# write to temp file to pipe into parallel
transfers = get_files( client, batch_size=BATCH )
#logging.info("TRANSFERS: %s" % (transfers,))
for target, files in iter(transfers.items()):
  copied = []
  with NamedTemporaryFile(dir='/tmp', prefix='redis-copy.', delete=True ) as f:
    f.write( ( '\n'.join( files ) + '\n' ).encode(encoding='UTF-8') )
    f.flush()
    logging.info("TRANSFER: %s" % (files,))
    cmd = "cat %s | grep -vE '^$' | SHELL=/bin/sh parallel --gnu --linebuffer --jobs=%s 'rsync -av %s%s {} %s/{//}/'" % ( f.name, PARALLEL, '--dry-run' if DRY_RUN else '', CHMOD, target ) 
    logging.info(f">> {cmd}")
    out = subprocess.getoutput( cmd ) 
    logging.info( f"{out}" )
    # rsync will spit out filenames of stuff that copied, so we grep for these, and remove them from files
    for o in out.split('\n'):
      if 'sending incremental file list' in o \
        or ' bytes/sec' in o \
        or o == '' \
        or 'total size is ' in o:
        continue;
      #logging.info(" copied over: %s" % (o,) )
      copied.append( o )
  logging.info("COPIED: %s" % (copied,))
