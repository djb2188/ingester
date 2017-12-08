# HealthPro CSV Importer

Service to take a CSV exported from HealthPro (software which is used by NIH _All of Us_ researchers) and import it into a database table. 

Written by Sean Pompea (http://seanpompea.sdf.org).

## Process
Written in Python 2.7, this software runs as a server process. It watches for a CSV to be deposited in the specified *inbox* folder. It replaces the destination database table with the contents of the CSV, then stores that CSV in the *archive* folder.


## Configuration / First-Time Setup

* Uses Python 2.7.10 or similar.
* A *virtualenv* setup is recommended.
* Ensure Python can talk to MS SQL Server (see:  https://github.com/seanpompea/pymssqlcheck)

After creating a _virtualenv_ (recommended), install by running:

    pip install -r requirements.txt --process-dependency-links --upgrade

### enclave folder

TODO update config format

Create a folder named ````enclave```` which will be a subdir of your working directory. Put ````healthproimporter_config.json```` in it. Customize the contents:

    { "inbox_dir" : "/path/to/inbox"
    , "archive_dir" : "/path/to/archive"
    , "consortium_tag" : "CONSORTIUM"
    , "db_info" : { "host" : "X"
                  , "user" : "X" 
                  , "password" : "X" }
    , "db_name"  : "dm_aou"
    , "db_schema" : "dbo"
    , "db_table" : "healthpro_dev"
    , "from_email" : "X"
    , "to_email" : "X"
    }

* The email addresses are for error and success notification emails.
* You can customize the table name; it should match a table that's been created (see below).

### archive and inbox folder

Create the archive and inbox folders as specified in the config json file (see above).

### Create database table

Create the database table in SQL Server using the DDL located in thed ````sql```` folder. Customize the database, schema, and table names to suit.

## Notes
### Column Names ###

`column-names.csv` is current as of HealthPro 0.8.7 (released the weekend of Nov 18th).

### Technical Details ###

Use Watchdog's PollingObserver (rather than the vanilla Observer) to observe files being delivered on a Samba share, NFS or similar.

## Back Matter

### Repository

https://github.com/seanpompea/healthproimporter


### Technical Notes

* Transact-SQL comamnds for running and monitoring SQL Server Agent jobs
  * sp_start_job -- https://docs.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-start-job-transact-sql
  * sp_help_job -- ttps://docs.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-help-job-transact-sql 

* Watchdog
  * https://pypi.python.org/pypi/watchdog
  * https://pythonhosted.org/watchdog/api.html#module-watchdog.events
  * https://pythonhosted.org/watchdog/api.html#module-watchdog.observers
  * https://stackoverflow.com/questions/24597025/using-python-watchdog-to-monitor-a-folder-but-when-i-rename-a-file-i-havent-b
  * https://pythonhosted.org/watchdog/api.html#watchdog.observers.polling.PollingObserver

* Unicode in Python 2
  * https://pythonhosted.org/kitchen/unicode-frustrations.html
  * http://farmdev.com/talks/unicode/

