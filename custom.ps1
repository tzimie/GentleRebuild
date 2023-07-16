function VictimClassifier([string] $conn, [int] $spid) {

  # returns:
  #  cmd - details of the blocked command
  #  cat - category this connection falls into
  #  waittime - time this connection already waited, sec
  #  maxwaitsec - maximum time for this connection allowed to wait

  # $waitdescr, $waitcategory, $waited, $waitlimit 
  $q = @"
  declare @cmd nvarchar(max), @job sysname, @chain int
  select @job=J.name from msdb.dbo.sysjobs J
    inner join ( 
    select 
      convert(uniqueidentifier, SUBSTRING(p, 07, 2) + SUBSTRING(p, 05, 2) +
      SUBSTRING(p, 03, 2) + SUBSTRING(p, 01, 2) + '-' + SUBSTRING(p, 11, 2) + SUBSTRING(p, 09, 2) + '-' +
      SUBSTRING(p, 15, 2) + SUBSTRING(p, 13, 2) + '-' +  SUBSTRING(p, 17, 4) + '-' + SUBSTRING(p, 21,12)) as j
    from (
    select substring(program_name,charindex(' 0x',program_name)+3,100) as p
      from sysprocesses where program_name like 'SQLAgent - TSQL JobStep%' and spid=$spid) Q) A
    on A.j=J.job_id
  select @cmd=Event_Info from sys.dm_exec_input_buffer($spid, NULL)
  select @chain=count(*) from sysprocesses where blocked=$spid and blocked<>spid
  select distinct @cmd as cmd,isnull(@job,'') as job, @chain as chain, waittime/1000 as waittime,open_tran,rtrim(program_name) as program_name 
    from sysprocesses P where P.spid=$spid
"@  
  $vinfo = MSSQLscalar $conn $q
  $prg = $vinfo.program_name
  $cmd = $vinfo.cmd # top level command from INPUTBUFFER
  $job = $vinfo.job # job name if it is a job, otherwise ""
  $chain = $vinfo.chain # >0 if a process, locked by Rebuild, also locks other processes (chain locks)
  $waittime = $vinfo.waittime # already waited
  $trn = $vinfo.open_tran # has transaction open
  # build category based on info above
  $cat = "INTERACTIVE"
  if ($cmd -like '*UPDATE*STATISTICS*') { $cat = "STATS" }
  elseif ($job -like "*ImportInvoicesMerchantData*") { $cat = "CRITJOB" }
  elseif ($job -gt "") { 
    $cat = "JOB" 
    $cmd = "Job $job"
  }
  elseif ($prg -like "*Management Studio*") { $cat = "STUDIO" }  
  elseif ($prg -like "SQLCMD*") { $cat = "SQLCMD" }  
  if ($chain -gt 0) { $cat = "L-" + $cat }    # prefix L- is added when there are chain locks, it is critical
  elseif ($trn -gt 0) { $cat = "T-" + $cat }  # prefix T- means that locked process is in transaction
  $cmd = $prg + " " + $cmd

  # allowed wait time before it thrrottles or kills rebuild
  $maxwaitsecs = @{
    "INTERACTIVE"   = 30
    "STATS"         = 36000
    "JOB"           = 600
    "CRITJOB"       = 60
    "STUDIO"        = 3600
    "SQLCMD"        = 600
    "T-INTERACTIVE" = 30
    "T-STATS"       = 600
    "T-JOB"         = 600
    "T-CRITJOB"     = 30
    "T-STUDIO"      = 1800
    "T-SQLCMD"      = 600
    "L-INTERACTIVE" = 20
    "L-STATS"       = 20
    "L-JOB"         = 20
    "L-CRITJOB"     = 20
    "L-STUDIO"      = 20
    "L-SQLCMD"      = 20
  } 
  return $cmd, $cat, $waittime, $maxwaitsecs[$cat]
}

function CustomThrottling([string] $conn) {

  # return values:
  #   0 - no throttling
  #   1 - soft throttling, between operations only
  #   2 - hard throttling, interrupts resumable operation 
  #   3 - panic, abort and exit

  # in the example below we check that job not slowed down an the last completion was not older than 600 sec
  $other = 0
<#
  $jobname = "MSX - MustRunQuickly" 
  $lastrunage = 600
  $extraq = @"
  select datediff(ss,max(msdb.dbo.agent_datetime(run_date,run_time)),getdate())
  from msdb.dbo.sysjobhistory where step_id=0 and job_id in (
    select job_id from msdb.dbo.sysjobs where name='$jobname')
"@
  $extraval = MSSQLscalar $conn $extraq
  $extraval = $extraval[0]
  Write-host "  Last executon of $jobname was $extraval sec ago"
  if ($extraval -gt $lastrunage) { $other = 1 }  # yes, throttle
#>
  return $other
}
