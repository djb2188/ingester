# HealthPro CSV Importer

Service to take a CSV exported from HealthPro and import it into a database table.

## Process

TODO description and diagram

## Configuration
Create a folder named ````enclave```` which will be a subdir of your working directory. Put ````healthproimporter_config.json```` in it. Contents:

	{ "inbox_dir" : "/path/to/inbox"
	, "archive_dir" : "/path/to/archive"
	, "institution_tag" : "MYINSTITUTION"
	, "db_info" : { "host" : "X"
	              , "user" : "X" 
	              , "password" : "X" }
	}

## Back Matter

### Repository

https://github.com/seanpompea/healthproimporter

### Technical Links

https://pypi.python.org/pypi/watchdog

https://pythonhosted.org/watchdog/api.html#module-watchdog.events

https://pythonhosted.org/watchdog/api.html#module-watchdog.observers

https://stackoverflow.com/questions/24597025/using-python-watchdog-to-monitor-a-folder-but-when-i-rename-a-file-i-havent-b
