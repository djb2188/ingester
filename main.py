import os
import sys 
import shutil
import time
import json
import csv
import codecs
# SQL Server connectivity:
import pymssql
# Watchdog library: https://pythonhosted.org/watchdog/
from watchdog.observers.polling import PollingObserver
from watchdog.events import FileSystemEventHandler
# Internal libraries we wrote:
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

#-----------------------------------------------------------------------------
# The next 2 statements prevent the following error when inserting into db:
#   "'ascii' codec can't encode character ..."
# Reference: https://stackoverflow.com/a/31137935
reload(sys)
sys.setdefaultencoding('utf-8')

#-----------------------------------------------------------------------------
# init

# colummn titles + 4 non-csv rows that HealthPro always includes
HP_CSV_EXTRANEOUS_ROWCOUNT = 5

# Create log object.
log = ks.create_logger('hpimporter.log', 'main')

# Load configuration info from config file.
config_fname = 'enclave/healthproimporter_config.json'
cfg = {}
with open(config_fname, 'r') as f: cfg = json.load(f)
consortium_tag = cfg['consortium_tag']
inbox_dir = cfg['inbox_dir']
archive_dir = cfg['archive_dir']
db_info = cfg['db_info']
db_name = cfg['db_name']
db_schema = cfg['db_schema']
db_table = cfg['db_table'] 
from_email = cfg['from_email'] 
to_email = cfg['to_email'] 

#------------------------------------------------------------------------------
# general utils

def ts():
  '''Return current timestamp in milliseconds (as an int).'''
  return int(round(time.time() * 1000))

def complete_tbl_name():
  '''Returns string; eg. [dm_aou].[dbo].[healthpro]
  This is based on what's in config'''
  return '[' + db_name + '].[' + db_schema + '].[' + db_table + ']'

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
# file utils

def del_file(f):
  log.info('About to delete ' + f)
  os.remove(f)
  log.info('Deleted ' + f)
  return True

def move_file(src, dest):
  log.info('About to copy {} to {}.'.format(src, dest))
  shutil.copy(src, dest)
  log.info('Copied {} to {}.'.format(src, dest))
  del_file(src)
  return True

#------------------------------------------------------------------------------
# db utils

def db_qy(qy):
  '''Run a SQL query. Returns list of maps.'''
  with pymssql.connect(**db_info) as conn:
    cursor = conn.cursor(as_dict=True)
    cursor.execute(qy)
    return cursor.fetchall()

def db_stmt(stmt):
  '''Execute a SQL DDL/DML statement. Returns bool. Throws.'''
  try:
    with pymssql.connect(**db_info) as conn:
      cursor = conn.cursor()
      cursor.execute(stmt)
      conn.commit()
  except Exception, e:
    log.error(str(e))
    raise e

def prep_insert_stmt(table_name, data):
  '''Takes a list of maps. Returns a ready-to-run SQL statement as a str.'''
  stmt = u''
  try:
    stmt = ( u''
           + "insert into [" + db_name + "].[" + db_schema + "].[" 
           + table_name + "] ([" 
           + "],[".join(data) 
           + "]) values ('"
           + "','".join(unicode(x).replace("'", "''") for x in data.values())
           + "')")
  except Exception, ex:
    log.error(str(ex))
    raise ex
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
# file checks 

def is_sys_file(fpath):
  '''Is this an OS-generated file, such as .DS_Store, etc?'''
  # Default to lowercasing things.
  sysfiles = ['.ds_store', 'desktop.ini', 'thumbs.db']
  x = os.path.basename(fpath).lower()
  return x in sysfiles 

def is_csv(fpath):
  return fpath.endswith('.csv')

def file_has_healthpro_naming_format(fpath):
  '''Confirm filename has the format we expect:
    - has .csv extension
    - contains consortium name we expect (from config).'''
  fname = os.path.basename(fpath)
  x = fname.startswith('workqueue_' + consortium_tag)
  y = is_csv(fpath)
  return (x and y)

def check_hp_csv_format(fpath):
  '''A HealthPro CSV has some extraneous lines at the beginning and end.
  Ensure this is the case with the current file.
  SPECIAL NOTE ON ENCODING: the HealthPro CSV is encoding as UTF-8 and also
  starts with a BOM (byte order marker). (Btw, the easiest way to confirm this
  is by opening the csv in vim.) The precise encoding in Python for
  this is not utf_8 but rather utf_8_sig. 
  For example, if you used utf-8, then the first row would have 120 chars
  (which includes the BOM), whereas the variable first_row has only 119 chars.
  See: https://docs.python.org/2/library/codecs.html
  '''
  first_row = u'"This file contains information that is sensitive '\
              'and confidential. Do not distribute either the file or '\
              'its contents."'
  second_row = u'""'
  penultimate_row = u'""'
  last_row = u'"Confidential Information"'
  with codecs.open(fpath, 'r', encoding='utf_8_sig') as f:
    rows = [line.strip() for line in f]
    a = rows[0] == first_row
    b = rows[1] == second_row
    c = rows[-2] == penultimate_row
    d = rows[-1] == last_row
  return (a and b and c and d)

def db_curr_rowcount():
  '''Returns int.'''
  qy = 'select count(*) as count from ' + complete_tbl_name()
  rslt = db_qy(qy)
  return rslt[0]['count']

def check_csv_rowcount(fpath):
  '''Rows in CSV must be >= rows in db.'''
  db_rowcount =  db_curr_rowcount()
  csv_rowcount = -1
  with codecs.open(fpath, 'r', encoding='utf_8') as f:
    # Note: rows in f will be of type unicode.
    csv_rowcount = sum(1 for row in f) - HP_CSV_EXTRANEOUS_ROWCOUNT
  return csv_rowcount >= db_rowcount

def check_csv_column_names(data, db_info, table_name):
  '''Column names must match what's in db.'''
  pass

def do_csv_checks(fname):
  '''Do all sanity checks on the newly deposited file.'''
  pass

#------------------------------------------------------------------------------
# csv handling

def standardize_healthpro_csv(fname):
  '''HealthPro CSV contains two extraneous lines at the start and end.
  Create a temp csv with these lines removed. Returns name of temp file.'''
  tmpfname = 'tmp' + str(ts()) + '.csv'
  lines = []
  with open(fname, 'r') as inn:
    log.info('Opened ' + fname + ' for reading.')
    lines = [line for line in inn]
  with open(tmpfname, 'w') as out:
    log.info('Opened ' + tmpfname + ' for writing.')
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
  stmt = 'truncate table [' + db_name + '].[' + db_schema + '].[' + table_name + ']'
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
    # sleep to ensure process writing to file is finished before we start.
    time.sleep(10)
    data = handle_csv(path)
    log.info('Handled csv successfully; about to load into database.')
    load_data_into_db(db_table, data)
    log.info('Successfully loaded into database.')
    log.info('Processed ' + path + ' successfully!')
    send_success_email()
  except Exception, ex:
    send_error_email('process_file: ' + str(ex))
    log.error(str(ex))

def make_fs_event_handler_obj(on_created_func):
  '''Create and return a new FileSystemEventHandler object (this class
  is part of the Watchdog library.)
  on_created_func should take one arg: a string (which will be the event 
  source path).'''
  pass

def main():
  print 'Starting main...'
  log.info('--------------------------------------------------------------------')
  log.info('HealthPro CSV Ingester service started.')
  log.info('Details about database from config file: Server: {}, Database: {}, '\
           'Schema: {}, Table: {}' \
           ''.format(db_info['host'], db_name, db_schema, db_table))
  observer = PollingObserver(timeout=5) # check every 5 seconds
  try:
    if not do_startup_checks():
      raise Exception('One or more startup checks failed')
    class FSEHandler(FileSystemEventHandler):
      def on_created(self, event):
        log.info('FSEHandler->on_created: a new file has appeared: '
                 + event.src_path)
        process_file(event.src_path)
    observe_subdirs_flag = False
    observer.schedule(FSEHandler(), inbox_dir, observe_subdirs_flag)
    observer.start()
    log.info('Waiting for activity...')
    print 'Service started.' 
    try:
      while True: time.sleep(1)
    except KeyboardInterrupt:
      observer.stop()
      sys.exit(0)
    observer.join()
  except Exception, ex:
    log.error(str(ex))
    send_error_email(str(ex))
    observer.stop()
    sys.exit(1)

if __name__ == '__main__': main()

