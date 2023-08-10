# this is settings file for gentle rebuild

# where to rebuild, MUST CHANGE!!!
$server = "your server" # must change
$dbname = "db1,db2" # * means all 9except system databases and ReportServer*), comma-separated list is also accepted
$workdb = "TOCHANGE" # chaneg to your DBA database, where FRG_ objects are installed

# values below work well as defaults
# what to rebuild
$reorganize = 0 # 1 to do INDEX REORGANIZE instead of rebuild
$allowNonResumable = 50 # Gb. if Resumable is not possible, do it without Resumable option. If table is begger, then skip
$allowNonOnline = 10 # Gb. if ONLINE is not possible, do it without ONLINE option. If table is begger, then skip
$threshold = 40 # rebuild if fragmentation percent is above
$extrafilter = " and SchemaName not in ('tmp','import') and page_count>1000 " # must be blank or start with <space>and ...

# options
$deadline = "" # blank or format "HH:mm". When time already passed today, tomorrow is assumed. "" for no deadline
$rebuildopt = "DATA_COMPRESSION=PAGE,MAXDOP=1" # Additional options, for example, MAXDOP, don't use NOT MAX_DURATION
$columnstoreopt = "MAXDOP=1" # options for for column store tables
$reorganizeopt = "" # for index reorganize
$relaxation = 10 # period we wait before attempts, giving time for other processes to complete
$maxcpu = 80 # throttles when cpu is above this level
$maxlogused = 50000 # Mb max log used throttling, 0 - no throttling
$maxlogusedpct = 0 # max log pct used throttling, 0 - no throttling
$maxqlen = 25000 # Mb max queue length of AlwaysOn to throttle. To disable set very big value
$maxdailysize = 0 # Mb max indexes rebuilt daily (to avoid generationg huge LDF backups), 0 if not limited
$chunkminutes = 60 # minutes, max period of continuous work, this is an artificial MAX_DURATION. use it, not SQL server setting
$killnonresumable = 1 # allow non resumable operations being killed (when they lock other processes) with losing all work
$forceoffline = 0 # enterprise version works as standard, ONLINE=On and RESUMABLE=ON are not used
$sortintempdb = 100000 # Mb, for tables below threashold use SORT_IN_TEMPDB=ON. For OFFLINE rebuilds only
$offlineretries = 3 # for nonresumable operations, when operation is killed, retries this number fo times
  
####    