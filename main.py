import os
import sys 
import shutil
import time
import json
import csv
import pymssql
#from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver
from watchdog.events import FileSystemEventHandler
import kickshaws as ks # logging, email

#-----------------------------------------------------------------------------#
#                                                                             #
#                   ---===((( healthproimporter )))===---                     #
#                                                                             #
#-----------------------------------------------------------------------------#
#
# This program is designed to run as a daemon. It imports a HealthPro CSV
# into a database table.
#
# Prior to running, you should configure the following:
#   o a folder where a HealthPro CSV can be deposited
#   o archive folder where CSVs will ultimately be stored
#   o MS SQL Server database table to hold the data
#
# Once running, it does this:
#   o Watches for a HealthPro CSV file to appear in the inbox folder (location
#       is configurable)
#   o Creates a temp version of the CSV that is slightly cleaned up
#   o Reads the CSV into memory
#   o Moves file to the archive folder (location also configurable)
#   o Truncates the  database table (database and table name are configurable)
#   o Inserts the new data into the table 
#   o If issue arises, email is sent to engineer
#
# Please see README.md for details.

#------------------------------------------------------------------------------
# init

# Create log object.
log = ks.create_logger('hpimporter.log', 'core')

# Load configuration info from config file.
config_fname = 'enclave/healthproimporter_config.json'
cfg = {}
with open(config_fname, 'r') as f: cfg = json.load(f)
institution_tag = cfg['institution_tag']
inbox_dir = cfg['inbox_dir']
archive_dir = cfg['archive_dir']
db_info = cfg['db_info']
db_table = cfg['db_table'] 
from_email = cfg['from_email'] 
to_email = cfg['to_email'] 

#------------------------------------------------------------------------------
# email

def send_success_email():
  try:
    ks.send_email(from_email, to_email, 'HealthPro WQ Ingest Success'
                 , 'Success!')
  except Exception, ex:
    log.error('Error when trying to email: ' + str(ex))
 
def send_error_email(msg):
  try:
    ks.send_email(from_email, to_email, 'HealthPro WQ Ingest ERROR', msg)
  except Exception, ex:
    log.error('Error when trying to email: ' + str(ex))

#------------------------------------------------------------------------------
# general utils

def ts():
  '''Return current timestamp in milliseconds (as an int).'''
  return int(round(time.time() * 1000))

#------------------------------------------------------------------------------
# file utils

def del_file(f):
  os.remove(f)
  log.info('Deleted ' + f)
  return True

def move_file(src, dest):
  shutil.copy(src, dest)
  log.info('Copied ' + src + ' to ' + dest)
  del_file(src)
  return True

#------------------------------------------------------------------------------
# db utils

def db_qy(qy):
  '''Run a SQL query. Returns list of maps.'''
  log.info('About to run this query: ' + qy)
  with pymssql.connect(**db_info) as conn:
    cursor = conn.cursor(as_dict=True)
    cursor.execute(qy)
    return cursor.fetchall()

def db_stmt(stmt):
  '''Execute a SQL DDL/DML statement. Returns bool.'''
  log.info('About to run this statement: ' + stmt)
  try:
    with pymssql.connect(**db_info) as conn:
      cursor = conn.cursor()
      cursor.execute(stmt)
      conn.commit()
      return True
  except Exception, e:
    print str(e) 
    return False

def prep_insert_stmt(table_name, data):
  '''Takes a list of maps. Returns a ready-to-run SQL statement as a str.'''
  stmt = ("insert into [dm_aou].[dbo].[" + table_name + "] ([" 
         + "],[".join(data) 
         + "]) values ('"
         + "','".join(data.values())
         + "')")
  return stmt

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

def do_startup_checks():
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
# csv handling

#TODO
def is_healthpro_fname_format(fname):
  '''Confirm CSV has the format we expect:
    - has expected number of columns
    - first and last lines are non-CSV free text (which we remove later). '''
  pass

#TODO
def check_csv_rowcount():
  '''Rows in CSV must be >= rows in db.'''
  pass

#TODO
def check_csv_column_names(data, db_info, table_name):
  '''Column names must match what's in db.'''
  pass

#TODO
def do_csv_checks(fname):
  pass

def standardize_healthpro_csv(fname):
  '''HealthPro CSV contains two extraneous lines at the start and end.
  Create a temp csv with these lines removed. Returns name of temp file.'''
  tmpfname = 'tmp' + str(ts()) + '.csv'
  lines = []
  with open(fname, 'r') as inn:
    log.info('Opened ' + fname + ' for reading.')
    lines = [line for line in inn]
  with open(tmpfname, 'w') as out:
    log.info('Opened ' + fname + ' for writing.')
    for i in range(len(lines)):
      if i not in [0, 1, len(lines)-1, len(lines)-2]:
        out.write(lines[i])
  log.info('Successfully created ' + tmpfname) 
  return tmpfname

def csv_to_data(fname):
  '''Returns a list of maps.'''
  with open(fname, 'r') as f:
    log.info('Opened ' + fname)
    reader = csv.DictReader(f)
    log.info('Successfully read ' + fname + ' into memory.')
    return [row for row in reader]

def handle_csv(fname):
  '''Read CSV & return data as list of maps; also moves CSV to archive 
  folder.'''
  tmp_fname = standardize_healthpro_csv(fname)
  data = csv_to_data(tmp_fname)
  del_file(tmp_fname)
  move_file(fname, archive_dir)
  return data

#------------------------------------------------------------------------------
# load data into db

def db_trunc_table(table_name):
  stmt = 'truncate table [dm_aou].[dbo].[' + table_name + ']'
  db_stmt(stmt) 

def load_data_into_db(table_name, data):
  db_trunc_table(table_name)
  def load_single(mp):
    stmt = prep_insert_stmt(table_name, mp)
    db_stmt(stmt)
  map(load_single, data)

#------------------------------------------------------------------------------
# driver

def process_file(path):
  try:
    data = handle_csv(path)
    log.info('Handled csv successfully; about to load into database.')
    load_data_into_db(db_table, data)
    log.info('Successfully loaded into database.')
    print 'Processed ' + path + ' successfully!'
    log.info('Processed ' + path + ' successfully!')
    send_success_email()
  except Exception, ex:
    send_error_email('process_file: ' + str(ex))
    log.error(str(ex))

def main():
  print 'Starting main...'
  log.info('-------------------------------------------------------')
  log.info('              healthproimporter starting...            ')
  observer = PollingObserver()
  try:
    if not do_startup_checks():
      print 'One or more startup checks failed'
      raise Exception('One or more startup checks failed')
    class FSEHandler(FileSystemEventHandler):
      def on_created(self, event):
        process_file(event.src_path)
    observe_subdirs_flag = False
    observer.schedule(FSEHandler(), inbox_dir, observe_subdirs_flag)
    observer.start()
    print 'observer started.' 
    try:
      while True: time.sleep(1)
    except KeyboardInterrupt:
      observer.stop()
      sys.exit(0)
    observer.join()
  except Exception, ex:
    print str(ex)
    log.error(str(ex))
    send_error_email(str(ex))
    observer.stop()
    sys.exit(1)

if __name__ == '__main__': main()

