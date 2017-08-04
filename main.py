import os
import time
import json
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import kickshaws as ks # logging, email
import pymssql

#-----------------------------------------------------------------------------#
#                                                                             #
#                   ---===((( healthproimporter )))===---                     #
#                                                                             #
#-----------------------------------------------------------------------------#

# This program is designed to run as a daemon. It watches for a HealthPro
# CSV file to appear in the 'inbox' folder, then it imports that data into
# a SQL Server database table (and also moves it to the 'archive' folder.)
#
# Please see README.md for details on configuration files you need to set up
# and how to run.

#-----------------------------------------------------------------------------#
# init

log = ks.create_logger('hpimporter.log', 'core')

config_fname = 'enclave/healthproimporter_config.json'
cfg = {}
with open(config_fname, 'r') as f:
  cfg = json.load(f)
institution_tag = cfg['institution_tag']
inbox_dir = cfg['inbox_dir']
archive_dir = cfg['archive_dir']
db_info = cfg['db_info']

#------------------------------------------------------------------------------
# db low-level

def db_qy(qy):
  '''Returns list of maps.'''
  with pymssql.connect(**db_info) as conn:
    cursor = conn.cursor(as_dict=True)
    cursor.execute(qy)
    return cursor.fetchall()

def db_stmt(stmt):
  '''Execute a statement.'''
  pass
  
#------------------------------------------------------------------------------
# startup checks

def check_inbox_dir_exists():
  return os.path.exists(inbox_dir)

def check_archive_dir_exists_and_writable():
  return (os.path.exists(archive_dir)
          and os.access(archive_dir, os.W_OK | os.X_OK))

def check_db_can_connect():
  qy = 'select @@Version as version'
  rslt = db_qy(qy)
  # rslt should be list containing one map, with key of 'version'.
  return 'version' in rslt[0]

def do_sanity_checks():
  '''Log results of checks; return T/F.'''
  checks = [check_inbox_dir_exists
          , check_archive_dir_exists_and_writable
          , check_db_can_connect]
  for f in checks:
    if f():
      log.info('Check successful: ' + f.__name__)
    else:
      log.error('Check failed: ' + f.__name__)
      return False
  return True

#------------------------------------------------------------------------------
# file handling

def is_healthpro_fname_format(fname):
  '''Confirm the filename matches the format we expect, which is:
  TODO.'''
  pass

def put_in_archive(fpath):
  pass

def del_file(fpath):
  pass

#------------------------------------------------------------------------------
# csv checks

def check_csv_rows():
  '''Rows in CSV must be >= rows in db.'''
  pass

def check_csv_columns(data, db_info, table_name):
  '''Column names must match what's in db.'''
  pass

def do_csv_checks():
  pass


#------------------------------------------------------------------------------
# db loading

def db_trunc_table(db_info, table_name):
  pass

def db_insert_healthpro_data(data, db_info, table_name):
  pass

#------------------------------------------------------------------------------
# driver

def process_file(path):
  '''Steps:
  o Confirm the new file is one we care about.
  o Copy into archive folder.
  o Delete the original.
  o Load file into memory.
  o db: Truncate HealthPro db table.
  o db: Load new data into table.
  o Email status (success or failure).'''
  print('process_file called')
  pass

def main():
  try:
    if not do_startup_checks():
      pass
      # TODO throw RuntimeError or ?
    class FSEHandler(FileSystemEventHandler):
      def on_created(self, event):
        process_file(event.src_path)
    ob = Observer()
    observe_subdirs_flag = False
    ob.schedule(FSEHandler(), 'inbox', observe_subdirs_flag)
    ob.start()
    try:
      while True: time.sleep(1)
    except KeyboardInterrupt: ob.stop()
    ob.join()
  catch Exception, e:
    pass

if __name__ == '__main__': main()

