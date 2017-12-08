import os # https://docs.python.org/2/library/os.path.html
import sys 
import shutil
import time
import json
import csv
import codecs
import chardet
import pymssql
from watchdog.observers.polling import PollingObserver
from watchdog.events import FileSystemEventHandler
import kickshaws as ks # logging, email

#-----------------------------------------------------------------------------#
#                              healthproimporter                              #
#-----------------------------------------------------------------------------#
# See README for details.

#-----------------------------------------------------------------------------
# The next 2 statements prevent the following error when inserting into db:
#   "'ascii' codec can't encode character ..."
# Reference: https://stackoverflow.com/a/31137935
reload(sys)
sys.setdefaultencoding('utf-8')

#-----------------------------------------------------------------------------
# init

# Total of [row w/ col titles] + [4 non-csv rows that HealthPro csv includes]
HP_CSV_NONDATA_ROWCOUNT = 5

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
healthpro_table_name = cfg['healthpro_table_name'] 
redcap_table_name = cfg['redcap_table_name']
redcap_job_name = cfg['redcap_job_name']
from_email = cfg['from_email'] 
to_email = cfg['to_email'] 

# Flag to indicate monitored folder is gone. 
inbox_gone_flag = False

#------------------------------------------------------------------------------
# general utils

def ts():
  '''Return current timestamp in milliseconds (as an int).'''
  return int(round(time.time() * 1000))

def slurp(pth):
  '''Example: make_test_table_sql = slurp('./sql/createtable.sql')'''
  out = None
  with open(pth) as f:
    out = f.read()
  return out

#------------------------------------------------------------------------------
# email

def send_success_email():
  try:
    ks.send_email(from_email, to_email, 'HealthPro WQ Ingest Success'
                 , 'Success!')
  except Exception, ex:
    log.error('Error when trying to email: ' + str(ex))

def send_notice_email(msg):
  try:
    ks.send_email(from_email, to_email, 'NOTIFICATION - HealthPro WQ Ingest', msg)
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
# db 

def db_qy(qy):
  '''Run a SQL query. Returns list of maps.'''
  try:
    with pymssql.connect(**db_info) as conn:
      cursor = conn.cursor(as_dict=True)
      cursor.execute(qy)
      return cursor.fetchall()
  except Exception, ex:
    log.error(str(ex))
    raise e

def db_stmt(stmt):
  '''Execute a SQL DDL/DML statement. Doesn't return anything. Throws.'''
  try:
    with pymssql.connect(**db_info) as conn:
      cursor = conn.cursor()
      cursor.execute(stmt)
      conn.commit()
  except Exception, e:
    log.error(str(e))
    raise e

def db_trunc_table(table_name):
  stmt = 'truncate table ' + table_name
  db_stmt(stmt) 

def db_executemany(stmt, tuples):
  '''Execute a parameterized statement for a collection of rows.
  tuples should be a sequence of tuples (not maps). Throws.'''
  try:
    with pymssql.connect(**db_info) as conn:
      cursor = conn.cursor()
      cursor.executemany(stmt, tuples)
      conn.commit()
  except Exception, e:
    log.error(str(e))
    raise e

def parameterized_insert_stmt(table_name, data):
  '''data should be a sequence of maps.
  See: http://pymssql.org/en/stable/pymssql_examples.html'''
  stmt = u''
  try:
    stmt = ( u''
           + 'insert into ' + table_name
           + ' ([' 
           + '],['.join(data[0]) # All rows should have same keys.
           + ']) values ('
           + ','.join('%s' for _ in data[0])
           + ')')
  except Exception, ex:
    log.error(str(ex))
    raise ex
  return stmt

def db_insert_many(table_name, data):
  '''Takes a seq of maps. Doesn't return anything. Throws.'''
  try:
    stmt = parameterized_insert_stmt(table_name, data) 
    tuples = map(lambda mp: tuple([mp[k] for k in mp]), data)
    db_executemany(stmt, tuples)
  except Exception, ex:
    log.error(str(ex))
    raise ex

def db_start_job(job_name):
  '''Start a SQL Server Agent job. Returns immediately.'''
  try:
    with pymssql.connect(**db_info) as conn:
      cursor = conn.cursor()
      cursor.callproc('msdb.dbo.sp_start_job', (job_name,))
      conn.commit()
  except Exception, e:
    log.error(str(e))
    raise e

def db_is_job_idle(job_name):
  '''job_name should be a SQL Server Agent job name. Returns boolean.'''
  result = []
  try:
    with pymssql.connect(**db_info) as conn:
      cursor = conn.cursor(as_dict=True)
      stmt = "exec msdb.dbo.sp_help_job @job_name=N'" + job_name + "'"
      cursor.execute(stmt) 
      result = cursor.fetchall()
  except Exception, e:
    log.error(str(e))
    raise e
  return result[0]['current_execution_status'] == 4 # 4 means idle.

def db_last_run_succeeded(job_name):
  '''job_name should be a SQL Server Agent job name. Returns boolean.'''
  result = []
  try:
    with pymssql.connect(**db_info) as conn:
      cursor = conn.cursor(as_dict=True)
      stmt = "exec msdb.dbo.sp_help_job @job_name=N'" + job_name + "'"
      cursor.execute(stmt) 
      result = cursor.fetchall()
  except Exception, e:
    log.error(str(e))
    raise e
  return result[0]['last_run_outcome'] == 1 # 4 means outome of succeeded.

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

def check_filename_format(fpath):
  '''Confirm filename has the format we expect:
    - has .csv extension
    - contains consortium name we expect (from config).'''
  fname = os.path.basename(fpath)
  x = fname.startswith('workqueue_' + consortium_tag)
  y = is_csv(fpath)
  return (x and y)

def check_char_encoding_is_utf8sig(fpath):
  '''Confirm that the character encoding of the file is utf-8 with
  a BOM (byte-order marker) -- in Python this is normally tagged as
  'utf_8_sig' or 'UTF-8-SIG', etc.
  See also: comment on function check_hp_csv_format below.'''
  f = open(fpath, 'rb') # read as raw bytes
  raw = f.read()
  rslt = chardet.detect(raw)
  enc = rslt['encoding']
  conf = str(rslt['confidence'])
  log.info('Detected character encoding for [{}] is [{}]; confidence: '\
           '[{}]'.format(fpath, enc, conf))
  return enc == 'UTF-8-SIG'

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

def db_curr_rowcount(table_name):
  '''Returns int.'''
  qy = 'select count(*) as count from ' + table_name 
  rslt = db_qy(qy)
  return rslt[0]['count']

def check_csv_rowcount(fpath):
  '''Rows in CSV must be >= rows in db.'''
  db_rowcount =  db_curr_rowcount(healthpro_table_name)
  csv_rowcount = -1
  with codecs.open(fpath, 'r', encoding='utf_8_sig') as f:
    # Note: rows in f will be of type unicode.
    csv_rowcount = sum(1 for row in f) - HP_CSV_NONDATA_ROWCOUNT
  return csv_rowcount >= db_rowcount

def get_target_column_names():
  out = []
  with codecs.open('column-names.csv', 'r', encoding='utf_8') as f:
    out = f.read().strip().split(',')
  return out

def check_csv_column_names(data):
  '''Column names must match what's in column-names.csv. Recall that
  data is a seq of maps.'''
  target_cols = get_target_column_names()
  csv_cols = data[0].keys() # Any row from data will do.
  return set(csv_cols) == set(target_cols)

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
# load healthpro and redcap data

def load_data_into_db(table_name, data):
  db_trunc_table(table_name)
  db_insert_many(table_name, data) 

def redcap_rowcount():
  return db_curr_rowcount(redcap_table_name)

# TODO the pattern here can be abstracted out for any 
# occasion we need to run a SQL Server job and poll for 
# it to finish.
def refresh_redcap_table():
  log.info('Before truncating REDCap table, rowcount is {}.' \
           ''.format(redcap_rowcount()))
  log.info('About to truncate REDCap data table...')
  db_trunc_table(redcap_table_name)
  log.info('Truncated REDCap data table; rowcount is {} (should be 0).' \
          ''.format(redcap_rowcount()))
  log.info('About to repopulate REDCap table from source by calling SQL ' \
          +'Server Agent job [{}].'.format(redcap_job_name))
  db_start_job(redcap_job_name)
  # Below we poll to see if the job has finished; when finished we check if
  # it was successful. If the job is still running past the designated
  # threshold, then we raise an exception, with the assumption that something
  # is wrong.
  waitfor = 480 # 8 minutes.
  interval = 10
  havewaited = 0
  while True:
    time.sleep(interval)
    if not db_is_job_idle(redcap_job_name):
      havewaited += interval
      log.info('The job [{}] is not idle yet; waiting ...' \
               ''.format(redcap_job_name))
      if havewaited > waitfor:
        raise Exception('Runtime for job [{}] has exceeded {}. Raising ' \
                        'exception.'.format(redcap_job_name, waitfor))
      else:
        continue
    else:
      break
  if not db_last_run_succeeded(redcap_job_name):
    msg = 'The job [{}] did not run successfully.'.format(redcap_job_name)
    log.error(msg)
    raise Exception(msg)
  else:
    log.info('Successfully ran [{}]. REDCap table rowcount: [{}].' \
            ''.format(redcap_job_name, redcap_rowcount()))
    return True 
  
#------------------------------------------------------------------------------
# driver

def process_file(path):
  log.info('----------process_file called------------------------------------')
  try:
    # sleep to ensure process writing to file is finished before we start.
    # E.g., if downloading directly into inbox folder using a browser, the
    # file will not be all there initially.
    time.sleep(30)
    # Should we ignore this file?
    if is_sys_file(path):
      return    
    # Do some sanity checks on the file.
    fname = os.path.basename(path) 
    if not check_filename_format(path):
      msg = 'The deposited file ({}) has an unexpected filename; '\
            'so, it was not processed. Please check.'.format(fname)  
      log.info(msg)
      send_notice_email(msg)
      return
    if not check_char_encoding_is_utf8sig(path):
      msg = 'The character encoding of the deposited CSV ({}) '\
            'doesn\'t match what\'s '\
            'expected; so, it was not processed. One way this could happen '\
            'is if the file were opened in Excel or another application and then '\
            'saved from within that application (even when saved as a CSV).'\
            ''.format(fname)  
      log.info(msg)
      send_notice_email(msg)
      return
    if not check_hp_csv_format(path):
      msg = 'The format of the deposited CSV ({}) doesn\'t match what\'s '\
            'expected; so, it was not processed. Please check.'.format(fname)  
      log.info(msg)
      send_notice_email(msg)
      return
    if not check_csv_rowcount(path):
      msg = 'The number of rows of data in the deposited CSV ({}) is fewer '\
            'than in the database; so, it was not processed. '\
            'Please check.'.format(fname)  
      log.info(msg)
      send_notice_email(msg)
      return
    # So far so good. Proceed to read in csv.
    data = handle_csv(path)
    log.info('Handled csv successfully')
    if not check_csv_column_names(data):
      msg = 'The columns in the deposited CSV ({}) don\'t match expectations; '\
            'so, it was archived but not processed. Please check.'.format(fname)  
      log.info(msg)
      send_notice_email(msg)
      return
    log.info('About to load into database.')
    load_data_into_db(healthpro_table_name, data)
    log.info('Successfully loaded into database.')
    csv_rowcount = len(data)
    db_rowcount = db_curr_rowcount(healthpro_table_name)
    log.info('Stats: csv rowcount: [' + str(csv_rowcount) + ']; '\
             'db rowcount: [' + str(db_rowcount) + '].')
    if csv_rowcount == db_rowcount:
      log.info('Processed ' + path + ' successfully!')
      # Now that we're done wtih the HealthPro side, refresh our REDCap data.
      if refresh_redcap_table():
        log.info('Refreshed REDCap data successfully!')
        log.info('Everything is done. Sending success email.')
        send_success_email()
      else:
        raise Exception('Something went wrong when refreshing REDCap data.')
    else:
      log.error('Rowcounts do not match.')
      send_error_email('Final rowcounts do not match; please check.')
  except Exception, ex:
    log.error(str(ex))
    send_error_email('An error occurred while processing {}. Please check.'.format(fname))

def make_handler_obj(on_created_func):
  '''Create and return a new FileSystemEventHandler object (this class
  is part of the Watchdog library.) which has custom handler functions
  specific to our needs.'''
  pass

def main():
  print 'Starting main...'
  log.info('--------------------------------------------------------------------')
  log.info('HealthPro CSV Ingester service started.')
  log.info('Details about database from config file: Server: {}, DB Table: {}, '\
           ''.format(db_info['host'], healthpro_table_name))
  observer = PollingObserver(timeout=5) # check every 5 seconds
  try:
    if not do_startup_checks():
      raise Exception('One or more startup checks failed')
    class FSEHandler(FileSystemEventHandler):
      # Here we define what we'd like to do when certain filesystem
      # events take place -- e.g., when a new CSV appears in the watched
      # directory.
      # Uncomment on_any_event for extra logging.
      #def on_any_event(self, event):
      #  log.info('FSEHandler->on_any_event: event_type=[' \
      #           '{}], src_path=[{}]'.format(event.event_type, event.src_path))
      def on_deleted(self, event):
        # Our forked Watchdog (v0.8.3.1) emits this event when inbox folder 
        # unmounts (or otherwise is not available).
        # We log and send an email only once. It very well might remount without 
        # us needing to do anything.
        global inbox_gone_flag
        if event.src_path == inbox_dir:
          if not inbox_gone_flag:
            inbox_gone_flag = True
            msg = event.src_path + ' is gone!'
            log.error(msg)
            send_notice_email(msg) 
      def on_created(self, event):
        # In on_deleted above, we set the inbox_gone_flag. But if a file appears
        # we know the inbox is back and all is well; so unset it. 
        global inbox_gone_flag
        if inbox_gone_flag:
          inbox_gone_flag = False
        log.info('FSEHandler->on_created: a new file has appeared: '
                 + event.src_path)
        process_file(event.src_path)
    observe_subdirs_flag = False
    observer.schedule(FSEHandler(), inbox_dir, observe_subdirs_flag)
    observer.start()
    log.info('Waiting for activity...')
    print 'Service started.' 
    try:
      while True:
        observer.join(10)
        if observer.is_alive():
          continue
        else:
          raise Exception('Observer thread has stopped.')
    except KeyboardInterrupt:
      print '\nKeyboard interrupt caught. Quitting.'
      observer.stop()
      sys.exit(0)
    observer.join() 
  except Exception, ex:
    print '\nError caught. Quitting. Check log.'
    log.error(str(ex))
    send_error_email('An error occurred in main(). Please check.')
    observer.stop()
    sys.exit(1)

#-----------------------------------------------------------------------------

if __name__ == '__main__': main()

