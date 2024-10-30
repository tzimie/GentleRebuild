param ([string] $settingfile)

function MSSQLquery([string] $connstr, [string]$sql) {
  $sqlConn = New-Object System.Data.SqlClient.SqlConnection
  $sqlConn.ConnectionString = $connstr
  $sqlConn.Open()
  $sqlcmd = New-Object System.Data.SqlClient.SqlCommand
  $sqlcmd.Connection = $sqlConn
  $sqlcmd.CommandText = $sql
  $sqlcmd.CommandTimeout = 1000000
  $adp = New-Object System.Data.SqlClient.SqlDataAdapter $sqlcmd
  $data = New-Object System.Data.DataSet
  $adp.Fill($data) | Out-Null
  #$_.Exception.Message | Out-File "$settingfile.log" -Append  
  $d = $data.Tables[0]
  $sqlConn.Close()
  return $d 
}

function MSSQLscalar([string] $connstr, [string]$sql) {
  $sqlConn = New-Object System.Data.SqlClient.SqlConnection
  $sqlConn.ConnectionString = $connstr
  $sqlConn.Open()
  $sqlcmd = New-Object System.Data.SqlClient.SqlCommand
  $sqlcmd.Connection = $sqlConn
  if ($sql.Contains("ALTER INDEX")) { $sqlcmd.CommandText = "SET DEADLOCK_PRIORITY LOW; $sql" }
  else { $sqlcmd.CommandText = $sql }
  $sqlcmd.CommandTimeout = 1000000
  $adp = New-Object System.Data.SqlClient.SqlDataAdapter $sqlcmd
  $data = New-Object System.Data.DataSet
  $adp.Fill($data) | Out-Null
  #$_.Exception.Message | Out-File "$settingfile.log" -Append  
  $firstrow = $data.Tables[0][0]
  $sqlConn.Close()
  return $firstrow
}

function MSSQLexec([string] $connstr, [string]$sql) {
  $sqlConn = New-Object System.Data.SqlClient.SqlConnection
  $sqlConn.ConnectionString = $connstr
  $sqlConn.Open()
  $sqlcmd = New-Object System.Data.SqlClient.SqlCommand
  $sqlcmd.Connection = $sqlConn
  $sqlcmd.CommandText = $sql
  $sqlcmd.CommandTimeout = 1000000
  $ret = $sqlcmd.ExecuteNonQuery()
  #$_.Exception.Message | Out-File "$settingfile.log" -Append  
  $sqlConn.Close()
  return $ret
}

function MSSQLexecQuick([string] $connstr, [string]$sql) {
  $sqlConn = New-Object System.Data.SqlClient.SqlConnection
  $sqlConn.ConnectionString = $connstr
  $sqlConn.Open()
  $sqlcmd = New-Object System.Data.SqlClient.SqlCommand
  $sqlcmd.Connection = $sqlConn
  $sqlcmd.CommandText = $sql
  $sqlcmd.CommandTimeout = 15
  $ret = $sqlcmd.ExecuteNonQuery()
  #$_.Exception.Message | Out-File "$settingfile.log" -Append  
  $sqlConn.Close()
  return $ret
}

function LOG([string] $db, [string]$schema, [string]$table, [string]$index, [int]$par, [string]$op, [string]$msg) {
  $msg = $msg.replace("'", '"')
  $parr = "$par"
  if ($parr -eq "") { $parr = 0 }
  $sql = @"
insert into $Tuning.dbo.FRG_LOG (DbName,SchemaName,TableName,IndexName,Partition,Op,Message)
  values ('$db','$schema','$table','$index',$parr,'$op','$msg')
"@ 
  $sqlConn = New-Object System.Data.SqlClient.SqlConnection
  $sqlConn.ConnectionString = $connstr
  $sqlConn.Open()
  $sqlcmd = New-Object System.Data.SqlClient.SqlCommand
  $sqlcmd.Connection = $sqlConn
  $sqlcmd.CommandText = $sql
  $ret = $sqlcmd.ExecuteNonQuery()
  $sqlConn.Close()
  return
}

. $PSScriptRoot/custom.ps1

function THROTTLER([string] $db) {
  $q = @"
  USE [$db];
  declare @cpupct int
  DECLARE @ts_now bigint = (SELECT cpu_ticks/(cpu_ticks/ms_ticks) FROM sys.dm_os_sys_info WITH (NOLOCK)); 
    select @cpupct=avg([SQLCPU]) from (
    SELECT SQLProcessUtilization AS [SQLCPU]
    FROM (SELECT record.value('(./Record/@id)[1]', 'int') AS record_id, 
			record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') 
			AS [SystemIdle], 
			record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') 
			AS [SQLProcessUtilization], [timestamp] 
	  FROM (SELECT [timestamp], CONVERT(xml, record) AS [record] 
			FROM sys.dm_os_ring_buffers WITH (NOLOCK)
			WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR' 
			AND record LIKE N'%<SystemHealth>%') AS x) AS y
			where DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) >= dateadd(mi,-10,getdate())
			) z  
  SELECT 
    (select count(*) from master.dbo.sysprocesses where program_name like 'Defrag%') as spidcnt,
    convert(int,sum(size/128.0)) AS CurrentSizeMB,  
	  convert(int,sum(size/128.0 - CAST(FILEPROPERTY(name, 'SpaceUsed') AS INT)/128.0)) AS FreeSpaceMB,
    (select isnull(sum(log_send_queue_size)+sum(redo_queue_size),0)
      from sys.dm_hadr_database_replica_states 
      where database_id=DB_ID('$db')) as qlen,
    (select isnull(max(case when redo_queue_size>0 then datediff(ss,last_commit_time, getdate()) else 0 end),0)
      from sys.dm_hadr_database_replica_states 
      where database_id=DB_ID('$db')) as delays,
    @cpupct as cpupct
    FROM sys.database_files WHERE type=1  
"@  
  $logsize = MSSQLscalar $connstrREAL $q
  $qlen = [int] ($logsize.qlen / 1024)
  $logtotal = $logsize.CurrentSizeMB
  $spidcnt = $logsize.spidcnt
  $logfree = $logsize.FreeSpaceMB  
  $logused = $logtotal - $logfree
  $logpct = $logused * 100. / $logtotal
  $delay = $logsize.delays
  $logpctformatted = $logpct.tostring('###.##', [Globalization.CultureInfo]::CreateSpecificCulture('en-US'))
  $cpupct = $logsize.cpupct
  if ($cpupct -gt 100) { $cpupct = 100. } 

  Write-Host "Database $db LDF: Total $logtotal Mb, Free $logfree Mb, Used $logused Mb - $($logpctformatted)%"
  Write-Host "  CPU $cpupct%, AlwaysOn Queue $qlen Mb, Replica delay $delay sec, actual rebuild spids: $spidcnt"
  $other = CustomThrottling $connstrREAL 
  if ($other -eq 3) {
    Write-host -ForegroundColor Red "  PANIC from Custom throttling! Exit!"
  }
  return $logused, $logpct, $qlen, $other, $cpupct, $spidnct
}

function fKILL([string] $connstr, [string] $db, [int]$abortflag) {
  $r = MSSQLscalar $connstr "select isnull((select max(spid) from master.dbo.sysprocesses where program_name like 'Defrag%'),0) as spid"
  $conn = $r.spid
  $qq = @"
Use [$db];  
select   
  (select count(*) from master.dbo.sysprocesses where spid=$conn and program_name like 'Defrag%') as cnt,
  (select count(*) from sys.index_resumable_operations where state=0) as running,
  (select count(*) from sys.index_resumable_operations where state=1) as paused
"@
  if ($abortflag -eq -1) {
    $r = MSSQLscalar $connstr $qq
    $spidcnt = $r.cnt
    $running = $r.running
    $paused = $r.paused
    Write-Host "  spidcnt=$spidcnt, running=$running, paused=$paused"
    if (($running -gt 0) -and ($paused -gt 0)) {
      Write-host -ForegroundColor Red "Panic - something went wrong! Check sys.index_resumable_operations"
      exit
    }
    return    
  }
  Write-Host -ForegroundColor Yellow "kill $conn"
  if ($conn -gt 0) { MSSQLexec $connstr "kill $conn" }
  while ($True) {
    Start-Sleep -Seconds 1
    $r = MSSQLscalar $connstr $qq
    $spidcnt = $r.cnt
    $running = $r.running
    $paused = $r.paused
    Write-Host "  spidcnt=$spidcnt, running=$running, paused=$paused"
    if (($spidcnt -eq 0) -and ($running -eq 0)) { break }
  }
  # if abortflag ABORT resumable index operation
  if (($abortflag -eq 1) -and ($paused -gt 0)) {
    $r = MSSQLscalar $connstr "use [$db]; select sql_text from sys.index_resumable_operations"
    $sql = $r.sql_text
    $sql = ($sql -split " REBUILD ")[0] + " ABORT"
    $sql = "use [$db]; $sql"
    Write-host -ForegroundColor Yellow $sql
    $a = MSSQLexec $connstr $sql
  }
}

# returns point of no return value 
# note that for MAXDOP=n there are typically n+1 threads, so $maxdop=2 should never occur
function PNRheuristic([int]$maxdop, [string]$itype) {
  if ($maxdop -gt 9) { return 40. }
  if ($itype -eq "CLUSTERED") { $heur = 75., 63., 63., 55., 45., 43., 42., 40., 37. }
  else { $heur = 86., 74., 74., 70., 64., 55., 50., 45., 40. }
  return $heur[$maxdop - 1]
}

# returns pct progress for nonresumable operations
function PCTheuristic([string]$itype, [string]$cmd, [float]$logreads, [float]$writes, [int]$pages, [int]$frags, [float]$density) {
  if ($cmd.contains(" REORGANIZE")) {
    if ($itype -eq "CLUSTERED") {
      $val = ($writes - $logreads / 2000) / ($pages + 0.5 * $frags) * (800 + $density) / 900 / 1.7 * 100.0
      return $val + $global:reorg_pct_adder
    }
    $val = $writes / ($pages + 0.5 * $frags) / (130.0 + $density) * 230 / 2.18 * 100.0  # nonclustered
    return $val + $global:reorg_pct_adder
  }
  if ($cmd.contains("COMPRESSION=PAGE")) {
    if ($itype -eq "CLUSTERED") {
      return ($logreads - $writes * 2) / ($pages + 0.2 * $frags) / (150.0 + $density) * 250 / 1.14 * 100.0 
    }
    return $logreads / $pages / (50.0 + $density) * 50.0 / 2.9 * 100.0 # nonclustered
  }
  # no compression
  if ($itype -eq "CLUSTERED") {
    return $logreads / $pages / (50.0 + $density) * 150 / 3.0 * 100.0 
  }
  return $logreads / ($pages + 0.1 * $frags) / (50.0 + $density) * 150.0 / 2.74 * 100.0 # nonclustered
}


function ADEFRAG([string] $connstrX, [string]$sqlX, [string]$alter, `
    [string] $db, [string]$schema, [string]$table, [string]$index, [int]$par) {
  $async = {
    param($connstr, $sql)
    $connstr = $connstr + ";Application Name=Defrag"
    $sqlConn = New-Object System.Data.SqlClient.SqlConnection
    $sqlConn.ConnectionString = $connstr
    $sqlConn.Open()
    $sqlcmd = New-Object System.Data.SqlClient.SqlCommand
    $sqlcmd.Connection = $sqlConn
    $sqlcmd.CommandText = $sql
    $sqlcmd.CommandTimeout = 1000000
    try {
      $ret = $sqlcmd.ExecuteNonQuery()
      $sqlConn.Close()
      $res = ""
    }
    catch {
      $res = $_
    } 	
    $res
  }
  $j = Start-Job -Name "SubDefragger" -ScriptBlock $async -ArgumentList $connstrX, $sqlX
  # at this point defrag is running asynchronously, and we check if any connections are locked by it
  $waitq = @"
  declare @n int, @io int, @blk int, @b int, @spidcnt int
  select @n=max(spid),@spidcnt=count(*),@io=sum(physical_io) from master.dbo.sysprocesses where program_name like 'Defrag%'
  select @b=max(blocked) from master.dbo.sysprocesses where spid=@n and blocked>0 and blocked<>@n
  select @blk=max(spid) from master.dbo.sysprocesses where blocked=@n and blocked<>spid and spid<>@n
  select isnull(@n,0) as defrag, isnull(@io,0) as io, isnull(@blk,0) as blk, isnull(@b,0) as wfor,
    (select max(percent_complete) from [$db].sys.index_resumable_operations) as pct,
    isnull(logical_reads,0) as logreads,
    isnull(writes,0) as writes,
    @spidcnt as spidcnt
    from sys.dm_exec_requests where session_id=@n
"@

  $who = @"
declare @n int
select @n=max(spid) from master.dbo.sysprocesses where program_name like 'Defrag%';
select distinct P.spid from sysprocesses P where P.blocked=@n and P.spid<>@n
"@

  $who2 = @"
declare @n int, @b int
select @n=max(spid) from master.dbo.sysprocesses where program_name like 'Defrag%';
select @b=max(blocked) from master.dbo.sysprocesses where spid=@n and blocked>0 and blocked<>@n;
with Rjobs (name,step_name,spid) as (
  select J.name,JS.step_name,A.spid from msdb.dbo.sysjobsteps JS
  inner join msdb.dbo.sysjobs J on JS.job_id=J.job_id
  inner join ( 
    select spid,
      convert(uniqueidentifier, SUBSTRING(p, 07, 2) + SUBSTRING(p, 05, 2) +
      SUBSTRING(p, 03, 2) + SUBSTRING(p, 01, 2) + '-' + SUBSTRING(p, 11, 2) + SUBSTRING(p, 09, 2) + '-' +
      SUBSTRING(p, 03, 2) + SUBSTRING(p, 01, 2) + '-' + SUBSTRING(p, 11, 2) + SUBSTRING(p, 09, 2) + '-' +
      SUBSTRING(p, 15, 2) + SUBSTRING(p, 13, 2) + '-' +  SUBSTRING(p, 17, 4) + '-' + SUBSTRING(p, 21,12)) as j
    from (
    select spid,substring(program_name,charindex(' 0x',program_name)+3,100) as p
      from sysprocesses where blocked=@n and blocked<>@n) Q) A
    on A.j=J.job_id)
select distinct P.spid,
  ltrim(rtrim(isnull('Job '+Rjobs.name+'\'+Rjobs.step_name,' '+program_name))+' '+rtrim(nt_username)) as activity
  from sysprocesses P 
  LEFT OUTER JOIN Rjobs on Rjobs.spid=P.spid
  where P.spid=@b
"@


  $started = Get-Date
  $session_started = Get-Date
  $oldio = -1
  $beinglocked = 0
  $stuckcnt = 0
  $globalcnt = 0
  $firstpctknown = -1
  $pollinterval = 1
  $maxspidcnt = 0
  if ($siz -gt 3) { $pollinterval = 5 } 
  if ($siz -gt 30) { $pollinterval = 15 }
  if ($pollinterval -gt $checkevery) { $pollinterval = $checkevery }
  
  while ((Get-Job -id $j.id).State -eq "Running") {
    $globalcnt = $globalcnt + 1
    Start-Sleep -Seconds $pollinterval
    $logreads = 0
    if ($itype -eq "CLUSTERED COLUMNSTORE") {
      $r = MSSQLscalar $connstrX @"
      use [$db];
      declare @n int, @io int, @blk int, @b int
      select @n=max(spid),@io=sum(physical_io) from master.dbo.sysprocesses where program_name like 'Defrag%'
      select @b=max(blocked) from master.dbo.sysprocesses where spid=@n and blocked>0
      select @blk=max(spid) from master.dbo.sysprocesses where blocked=@n and blocked<>spid and blocked<>@n
      select count(*) as cnt, isnull(@n,0) as defrag, isnull(@io,0) as io, isnull(@blk,0) as blk, isnull(@b,0) as wfor,
        (select case when ind>0 then 100.*old/ind else 0.0 end as pct from (
          select 1+sum(case when transition_to_compressed_state_desc='INDEX_BUILD' then 1 else 0 end) as old,
                 sum(case when transition_to_compressed_state_desc<>'INDEX_BUILD' then 1 else 0 end) as ind
          from sys.dm_db_column_store_row_group_physical_stats where object_id=object_id('[$schema].[$table]')) Q ) as pct     
"@
    }
    else {
      $r = MSSQLscalar $connstrX $waitq
      $logreads = $r.logreads
      $writes = $r.writes
      $spidnct = $r.spidcnt
    }
    $defragspid = $r.defrag
    $pct = $r.pct
    if ($pct -is [DBNull]) { $pct = 0.0 }
    $io = $r.io
    $blocked = $r.blk
    $wfor = $r.wfor

    if ([Console]::KeyAvailable) {
      $readkey = [Console]::ReadKey($true)
      if ($readkey.Modifiers -eq "Control" -and $readkey.Key -eq "C") {                
        Write-host ""
        if ($global:progress -gt "") { Write-host -ForegroundCOlor Yellow $global:progress } 
        Write-host -ForegroundCOlor Yellow "Current index rebuild: $cmd"
        Write-host -ForegroundCOlor Yellow "Enter 1-character command:"
        Write-host -ForegroundCOlor Yellow "  A - ABORT  - aborts index rebuild immediately"
        if ($cmd.contains("RESUMABLE=ON") -or $cmd.contains(" RESUME")) {
          Write-host -ForegroundCOlor Yellow "  S - STOP   - exits script, but leaves index in rebuild state"
        }
        if ($cmd.contains( "REORGANIZE")) {
          Write-host -ForegroundColor Yellow "  R - for REORGANIZE, stops processing and recalulates new stats"
        }
        Write-host -ForegroundCOlor Yellow "  F - FINISH - finishes current index rebuild and then stops"
        Write-host -ForegroundCOlor Yellow "  K - SKIP   - aborts (skips) this index but continues with the rest of the work"
        Write-host -ForegroundCOlor Yellow "  C - CONTINUE"
        $ky = (Read-Host "Command").ToUpper()
        [console]::TreatControlCAsInput = $true
        $Host.UI.RawUI.FlushInputBuffer()
        if ($ky -eq "F") { $global:finishflag = 1 }
        elseif ($ky -eq "R") { 
          fKILL $connstrX $db 1
          $global:finishflag = 3 
          break
        }
        elseif ($ky -eq "K") { 
          fKILL $connstrX $db 1
          $global:finishflag = 2 
          break
        }
        elseif ($ky -eq "S") { 
          fKILL $connstrX $db 0
          exit
        }
        elseif ($ky -eq "A") { 
          fKILL $connstrX $db 1 
          exit
        }
      }
      $Host.UI.RawUI.FlushInputBuffer()
    }    
	

    if ( $wfor -eq 0 ) { $beinglocked = 0 }
    if ($blocked -gt 0) {
      $newblocked = 0
      $pnr = 101. # pnr - point of no return
      try {
        if ( -not ($cmd.contains("RESUMABLE=ON") -or $cmd.contains(" RESUME") -or $cmd.contains(" REORGANIZE"))) {
          $pct = PCTheuristic $itype $cmd $logreads $writes $pages $frags $density
          # $pct = 100 * $logreads / 2.0 / ($pages + 3000)
          # if ($pct -gt 100) { $pct = 99.9 }
          if ($pct -lt 0) { $pct = 0.0 }
          $pnr = PNRheuristic $spidnct $itype
        } 
        $elapsed = (get-date) - $started
        [int]$left = ($elapsed.TotalSeconds / ($pct - $firstpctknown)) * (100 - $pct)
        if ($left -lt 0) { $left = 0 }
      }
      catch { $left = 0 }
      if ($pct -lt 0.1) { $left = 0 }
      if ($cmd.contains("RESUMABLE=ON") -or $cmd.contains(" RESUME") -or $cmd.contains(" REORGANIZE")) { $left = 0 }

      Write-Host -ForegroundColor Red "  the following processes are blocked and waiting: "
      $vics = MSSQLscalar $connstrX $who
      foreach ($victim in $vics) {
        $act, $waitcategory, $waited, $waitlimit = VictimClassifier $connstrX $victim.spid
        if (($waited + $left) -gt $waitlimit) { 
          Write-Host -ForegroundColor Yellow "    $waitcategory spid=$($victim.spid) waited $($waited)s + $($left)s projected (max $waitlimit) runs ($act)"
          $newblocked++ 
        }
        else {
          Write-Host "    $waitcategory spid=$($victim.spid) waited $($waited)s + $($left)s projected (max $waitlimit) runs ($act)"
        }
      }
      $blocked = $newblocked
    }
    if ($wfor -gt 0) {
      $beinglocked = $beinglocked + 1
      Write-Host -ForegroundColor Red "  defrag is blocked and is waiting for:"
      $vics = MSSQLscalar $connstrX $who2
      foreach ($victim in $vics) {
        $act = $victim.activity.replace("`n", ", ").replace("`r", ", ")
        if ($act -eq "") {
          $dbcc = MSSQLscalar $connstrX "DBCC INPUTBUFFER($($victim.spid))"
          $act = $dbcc.EventInfo.Trim()
        }
        Write-Host -ForegroundColor Yellow "    spid = $($victim.spid) runs ($act)"
      }
    }
    $chunkreason = 0
    if ($cmd.contains("RESUMABLE=ON") -or $cmd.contains(" RESUME")) {
      if (($chunkminutes -gt 0) -and (((get-date) - $session_started).TotalMinutes -gt $chunkminutes)) {
        Write-Host "  stopping current chunk ($chunkminutes minutes)..."
        $chunkreason = 1
        LOG $db $schema $table $index $par "PAUSE-KILL-CHUNK" $msg
        fKILL $connstrX $db 0
        break
      }
    }
    try {
      $left = ($elapsed.TotalSeconds / ($pct - $firstpctknown)) * (100 - $pct)    
    }
    catch {
      $left = 10
    }
    if (($blocked -gt 0) -or ($stuckcnt -gt 20)) {
      if (($blocked -eq 0) -and ($beinglocked -eq 0)) { 
        $msg = "process is stuck, no IO" 
      }
      else { $msg = "processes blocked: $blocked, worker spid=$defragspid" }
      LOG $db $schema $table $index $par "BLOCKED" $msg
      if (($io - $oldio) -gt 400) { $stuckcnt = 0 }
      elseif ( $beinglocked -eq 0 ) { $stuckcnt = $stuckcnt + 1 }
      Write-Host "  $msg"
      if ($cmd.contains("RESUMABLE=ON") -or $cmd.contains(" RESUME") -or $cmd.contains(" REORGANIZE")) {
        Write-Host "  pausing REBUILD/REORG..."
        LOG $db $schema $table $index $par "PAUSE-KILL" $msg
        fKILL $connstrX $db 0
        break
      }
      elseif ($pct -gt $pnr) {
        Write-host "  Current progress $pctformatted % is behind point of no return: $pnr%"
      }
      elseif ($killnonresumable -eq 1) {
        Write-host -ForegroundColor Yellow "  As KillNonResumable=1, connection will be killed and work will be lost"
        LOG $db $schema $table $index $par "KILL" $msg
        fKILL $connstrX $db 1
        break
      }
      else {
        Write-host -ForegroundColor Yellow "  Can't yield to lock without losing all work done (non resumable op). Use Ctrl/C for manual actions"
      }
    }

    $throttle = 0
    if (($globalcnt % 10) -eq 1) {
      $logused, $logpct, $qlen, $other, $cpupct, $spidcnt = THROTTLER $db
      if ($other -eq 3) {
        # panic
        LOG $db $schema $table $index $par "PAUSE-KILL" "PANIC"
        fKILL $connstrX $db 1
        exit
      }
      if ($spidcnt -gt $maxspidcnt) { 
        $maxspidcnt = $spidcnt 
        LOG $db $schema $table $index $par "MAXDOP" "$maxspidcnt"
      }      
      if ($cmd.contains("RESUMABLE=ON") -or $cmd.contains(" RESUME") -or $cmd.contains(" REORGANIZE")) {
        if ((($maxlogused -gt 0) -and ($logused -gt $maxlogused)) `
            -or (($maxlogusedpct -gt 0) -and ($logpct -gt $maxlogusedpct)) `
            -or ($qlen -gt $maxqlen) `
            -or ($cpupct -gt $maxcpu) `
            -or ($other -eq 2)) {
          if ($origcmd.contains("RESUMABLE=ON") -or $cmd.contains(" REORGANIZE")) {
            Write-Host -ForegroundColor Yellow "  starting to throttle because of LDF size or Queue size"
            LOG $db $schema $table $index $par "PAUSE-KILL" "throttling"
            fKILL $connstrX $db 0
            $throttle = 1
            break
          }
          else {
            Write-host -ForegroundColor Yellow "  LDF is too big, but operation is not resumable, we can't throttle it without losing the work done"
          }
        }
      }
    }
	
    if ($defragspid -gt 0) { 
      if ($pct -gt 0.0) {
        if ($firstpctknown -lt 0) {
          $started = get-date
          $session_started = get-date
          $firstpctknown = $pct 
        }
        else {
          try {
            $elapsed = (get-date) - $started
            $left = ($elapsed.TotalSeconds / ($pct - $firstpctknown)) * (100 - $pct)
            $etaval = $Null
            $etaval = (Get-Date).AddSeconds($left)
            $eta = $etaval.ToString("yyyy-MM-dd HH:mm:ss")
          }
          catch {
            $eta = "(unknown)"
          }
          $pctformatted = $pct.tostring('###.##', [Globalization.CultureInfo]::CreateSpecificCulture('en-US'))
          $msg = "  ETA: $eta   pct completed: $pctformatted%" 
          if ($stuckcnt -gt 0) { $msg = "$msg stuckcnt=$stuckcnt (max 20)" }
          if ($pctformatted -eq "100") { $beinglocked = 0 }
          if ($beinglocked -gt 0) { $msg = "$msg lockcnt=$beinglocked" }
          if ($deadline -gt "" -and $etaval -ne $Null -and $etaval -gt $dl) {
            Write-Host -ForegroundColor yellow $msg
          }
          else {
            Write-Host $msg
          }
          if ($deadline -gt "" -and $etaval -ne $Null -and $pct -gt 3 -and $pct -lt 4) {
            if ($etaval -gt $dl) {
              Write-Host -ForegroundColor yellow "Current index won't complete in time before the deadline ($deadline)"
              Write-Host -ForegroundColor yellow "Defrag Aborted. (this condition is verified when pct is between 3% and 4%)"
              fKILL $connstrX $db 1
              exit
            }
          }
          if ($deadline -gt "" -and $etaval -ne $Null -and $harddeadline -gt 0) {
            if ($etaval -gt $dl) {
              Write-Host -ForegroundColor yellow "Current index won't complete in time before the HARD deadline ($deadline)"
              fKILL $connstrX $db 1
              exit
            }
          }
        }
      }
      else {
        # no pct, OFFLINE REBUILD
        $elapsed = (get-date) - $started
        $pct = PCTheuristic $itype $cmd $logreads $writes $pages $frags $density
        #$pct = 100 * $logreads / 2.0 / ($pages + 3000)
        #if ($pct -gt 100) { $pct = 99.9 }
        if ($pct -lt 0) { $pct = 0.0 }
        if ($cmd.contains(" REORGANIZE")) {
          if ($pct -gt 120.0) {
            Write-host -ForegroundColor Red "During INDEX REORGANIZE, when pct is above 120.0, it could be a symptom of a 'collision'"
            Write-host -ForegroundColor Red "This is when it tries to reorganize a 'hotspot', where new data is constantly being inserted"
            Write-host -ForegroundColor Red "To avoid being stuck in a near-infinite loop, script stops processing at this point"
            fKILL $connstrX $db 1
            $global:finishflag = 3 
            break
          }
        }
        try { $left = (($elapsed.TotalSeconds + $global:reorg_elapsed_adder) / $pct) * (100 - $pct) } 
        catch { $left = 10 }
        if ("$left" -eq "NaN") { $left = 1 }
        if ($left -gt 100 * 24 * 3600) { $left = 24 * 3600 * 100 }
        $eta = (Get-Date).AddSeconds($left).ToString("yyyy-MM-dd HH:mm:ss")
        $pctformatted = $pct.tostring('###.##', [Globalization.CultureInfo]::CreateSpecificCulture('en-US'))
        Write-Host "  ETA: $eta   pct completed: $pctformatted% (rough estimation)" 
      }
    }
    $oldio = $io
    $global:global_pct = $pct
  }
  Wait-Job -Job $j | Out-null
  $res = [string](Receive-Job -Job $j)
  $res = $res.replace("`n", ", ").replace("`r", ", ")
  $status = 0
  if ($throttle -gt 0) {
    $res = "INDEX REBUILD is THROTTLED because of LDF size or Queue size"    
    $status = 6
  }
  elseif ($res.Contains("high priority DDL operation")) {
    $res = "INDEX REBUILD is PAUSED"
    $status = 2
  } 
  elseif ($res.Contains("process and has been chosen as the deadlock victim")) {
    $res = "Deadlocked and terminated"
    $status = 2
  } 
  elseif ($res.Contains("are currently in resumable index rebuild state")) {
    $res = "PANIC!"
    $status = 99
  } 
  elseif ($res.Contains("elapsed time exceeded") -or ($chunkreason -eq 1)) {
    $res = "Stopped because exceeded MAX_DURATION"
    $status = 3
  }
  elseif ($global:finishflag -eq 3) {
    $res = ""
    $status = 0
  }
  elseif ($global:finishflag -eq 2) {
    $res = "Index force skipped"
    $status = 7
  }
  elseif ($res.Contains("severe error occurred")) {
    $res = "REBUILD terminated because of locks"
    $status = 1
  }
  elseif ($res.Contains("Resumable index operation for index")) {
    $res = "Index is not rebuildable with RESUMABLE=ON"
    $status = 4
  }
  elseif ($res.Contains("The RESUMABLE option is not supported")) {
    $res = "Index is not rebuildable with RESUMABLE=ON"
    $status = 4
  }
  elseif ($res.Contains("failed because the index contains")) {
    $res = "Index is not rebuildable ONLINE"
    $status = 5
  }
  elseif ($res.Contains("An online operation cannot be performed for index")) {
    $res = "Index is not rebuildable ONLINE"
    $status = 5
  }
  elseif ($res.Contains("There is no pending resumable index operation for the index")) {
    $res = "Already finished"
    $status = 8
  }  
  if ($res -gt "") { Write-Host -ForegroundColor Red "  $res" }
  if ($res -eq "") {
    $status = 0
    LOG $db $schema $table $index $par "COMPLETED" ""
    Write-Host "Success!!! ($siz Mb) Recalculating stats..."
    if ($server -ne $replicaserver) {
      $secsync = 0
      $lsn = (MSSQLquery $connstrREAL "select last_hardened_lsn from sys.dm_hadr_database_replica_states where is_local=1 and database_id=db_id('$db')")[0]
      Write-Host "  Target  LSN: $lsn"
      while ($True) {
        $currlsn = (MSSQLquery $connstr "select last_commit_lsn from sys.dm_hadr_database_replica_states where is_local=1 and database_id=db_id('$db')")[0]
        Write-Host "  Current LSN: $currlsn   wait for LSN: $lsn"
        if ($currlsn -ge $lsn) { 
          Write-Host "  Synchronized after $secsync seconds"
          break 
        }
        Start-Sleep -Seconds 1
        $secsync += 1
      }
    }
    $sql = "exec $Tuning.dbo.FRG_FillFragmentationOne '$db','$schema','$table','$index',$par"
    try {
      $delta = MSSQLquery $connstr $sql
    }
    catch {
      # retry in case of deadlock
      $delta = MSSQLquery $connstr $sql
    }
    $before = $delta.Before
    $after = $delta.After
    $diff = $delta.delta
    Write-Host "Stats updated, $before Mb -> $after Mb, delta $diff Mb"
    (Get-Date -Format "yyyy-MM-dd HH:mm:ss") | Out-File "$settingfile.log" -Encoding utf8 -Append
    "$db $schema.$table, index $index, partition $par" | Out-File "$settingfile.log" -Encoding utf8 -Append
    "  stats updated, $before Mb -> $after Mb, delta $diff Mb" | Out-File "$settingfile.log" -Encoding utf8 -Append
  }
  else {
    LOG $db $schema $table $index $par "FAILED" $res
  }
  Remove-Job -Job $j | Out-Null
  $global:gresult = $status
  $global:reorg_pct_adder = $global:global_pct
  $global:reorg_elapsed_adder = $elapsed.TotalSeconds + $global:reorg_elapsed_adder 
  return $status
}

###############################################################################################
Write-Host -ForegroundColor Blue @"
 _____             _   _        _____      _           _ _     _ 
/ ____|           | | | |      |  __ \    | |         (_) |   | |
| |  __  ___ _ __ | |_| | ___  | |__) |___| |__  _   _ _| | __| |
| | |_ |/ _ \ '_ \| __| |/ _ \ |  _  // _ \ '_ \| | | | | |/ _  |
| |__| |  __/ | | | |_| |  __/ | | \ \  __/ |_) | |_| | | | (_| |
 \_____|\___|_| |_|\__|_|\___| |_|  \_\___|_.__/ \__,_|_|_|\__,_|
                                                                
"@
Write-Host -ForegroundColor Green "Version 1.37, github https://github.com/tzimie/GentleRebuild"

# where to defragment
# setting, defaults if setting file is not provided
$server = "servername"
$dbname = "db1,db2" # * means all, comma-separated list is also accepted
$replicaserver = ""

# what to defragment
$allowNonResumable = 50 # Gb. if Resumable is not possible, do it without Resumable option. If table is begger, then skip
$allowNonOnline = 10 # Gb. if ONLINE is not possible, do it without ONLINE option. If table is begger, then skip
$threshold = 40 # skip tables with low level of fragmentation
# must be blank or start with <space>and ...
$extrafilter = " and SchemaName not in ('tmp','import')"
$reorganize = 0 # 1 use use REORANIZE instead of REBUILD

# options
$starttime = "" # HH:mm wait start time (for maint window). "" start immediately
$deadline = "" # HH:mm when already passed today, tomorrow is assumed. "" for no deadline
$harddeadline = 0 # if 1, process is terminated no matter what stage
$rebuildopt = "DATA_COMPRESSION=PAGE,MAXDOP=2" # Additional options, for example, MAXDOP, but NOT MAX_DURATION and not SORT_IN_TEMPDB
$columnstoreopt = "MAXDOP=1" # for column store
$reorganizeopt = "" # for index reorganize
$relaxation = 50 # period we wait before attempts, giving time for other processes to complete
$checkevery = 15 # chack for locks every N seconds
$chunkminutes = 1000000 # max period of continuous work, this is an artificial MAX_DURATION. use it, not SQL server setting
$maxcpu = 80 # max cpu pct to throttle
$maxlogused = 200 * 1000 # Mb max log used throttling, 0 - no throttling
$maxlogusedpct = 0 # max log pct used throttling, 0 - no throttling
$maxqlen = 25000 # Mb max queue length of AlwaysOn to throttle. To disable set very big value
$maxdailysize = 1000 * 1000 # 500 * 1000 # Mb max indexes reorged daily, 0 if not limited
$killnonresumable = 0 # allow non resumable operations being killed with losing all work
$forceoffline = 0 # force non online rebuild
$offlineretries = 3 # number of retries because of locks before skipping an index
$sortintempdb = 100000 # Mb, for OFFLINE rebuilds, max size when SORT_IN_TEMPDB can be used. use 0 to disable SORT_IN_TEMPDB
$Tuning = "DBAdb"
$orderby = "TotalSpaceMb" # default, by size
# "frag_pct desc" -- from the worst
# "frag_count desc" -- from highest entropy
# "activity desc" -- most active first
# "activity*frag_count desc" -- extra workload for read access
# "activity*(100-density) desc" -- potential space benefit after rebuild


# get params
if ($settingfile -gt "") {

  . ("$PSScriptRoot/$settingfile.ps1")
  $Tuning = $Workdb
}

if ($replicaserver -eq '') { $replicaserver = $server }

if ($Tuning -eq "TOCHANGE") {
  Write-Host -ForegroundColor Red "It is unlikely that you DBA database is called 'TOCHANGE'. Please set workdb to DBA database where FRG script is installed"
  exit
}

Write-Host "Server=$server"
Write-Host "Database=$dbname"
if ($deadline -gt "") {
  $dl = [datetime]::ParseExact($deadline, 'HH:mm', $null)
  if ($dl -le (Get-Date)) {
    Write-Host "For the deadline $deadline, tomorrow is assumed!"
    $dl = $dl.AddDays(1)
  }
  if ($harddeadline -gt 0) 
    { Write-Host "HARD deadline: $dl" }
  else
    { Write-Host "Deadline: $dl" }
}

$connstr = "Server=$replicaserver;Database=$Tuning;Trusted_Connection=True;" # to DBA database, may be on RO replica
$connstrREAL = "Server=$server;Database=$Tuning;Trusted_Connection=True;" # to primary server
$r = MSSQLscalar $connstrREAL "select isnull((select max(spid) from master.dbo.sysprocesses where program_name like 'Defrag%'),0) as spid"
$fragspid = $r.spid
if ($fragspid -gt 0) {
  Write-Host -ForegroundColor Red "Defrag spid $fragspid is already running (label Defrag)"
  exit
}

$r = MSSQLscalar $connstr "select count(*) as cnt from sysobjects where name like 'FRG_%'"
if ($r.cnt -eq 0) {
  Write-Host -ForegroundColor Red "Please apply FRG_install.sql to your database [$Tuning]"
  exit
}

$r = MSSQLscalar $connstr "select count(*) as cnt from FRG_SizeStats"
if ($r.cnt -eq 0) {
  Write-Host -ForegroundColor Red "Please run FRG_FillSizeStats in database [$Tuning] to collect index size statistics"
  exit
}

$r = MSSQLscalar $connstr "select count(*) as cnt from FRG_Levels"
if ($r.cnt -eq 0) {
  Write-Host -ForegroundColor Red "Please run FRG_FillFragmentation in database [$Tuning] to collect index fragmentation statistics"
  exit
}

$ent = (MSSQLscalar $connstr @"
select case 
  when @@version like '%Enterprise%' then 1 
  when @@version like '%Developer%' then 1 
  else 0 end as ent
"@ ).ent
if ($ent -eq 0) { 
  Write-Host "Warning: Standard Edition detected" 
}
if ($forceoffline -eq 1) { $ent = 0 }

$where = " where 1=1 "
$where = $where + " and frag_pct>$threshold "
$where = $where + $extrafilter

if ($dbname -ne '*') { 
  if ($dbname.Contains(',')) { $where = $where + " and DbName in ('" + ($dbname.Split(',') -join "','") + "')" }
  else { $where = $where + " and DbName='$dbname'" }
}
else { $where = $where + " and DbName not in ('master','tempdb','model','msdb','ReportServer','ReportServerTempDb')" }
$q = "select count(*) as cnt,sum(rows) as rows,sum(TotalSpaceMb) as TotalSpaceMb from FRG_last $where"
Write-Host -Foreground Yellow $q
$row = MSSQLscalar $connstr $q
if ($row.cnt -eq 0) {
  Write-Host -ForegroundColor Yellow "Nothing to do !!!!!!!!!!!!!!!"
  exit
}
Write-Host "Total $($row.cnt) indexes with $([int]($row.rows/1000000))M rows, total size $([int]($row.TotalSpaceMb/1024))Gb"

if ($starttime -gt "") {
  $stt = [datetime]::ParseExact($starttime, 'HH:mm', $null)
  if ($stt -le (Get-Date)) {
    Write-Host "For the start time $starttime, tomorrow is assumed!"
    $stt = $stt.AddDays(1)
  }
  Write-Host "Waiting start time: $stt" 
  while ((get-date) -lt $stt) {
    Start-Sleep -Seconds 1
    }
}

LOG "" "" "" "" 0 "INIT" $where
$worksize = $row.TotalSpaceMb
$workdone = 0
$started = Get-Date
$session_started = get-Date

[console]::TreatControlCAsInput = $true
$Host.UI.RawUI.FlushInputBuffer()
$global:finishflag = 0
$q = "select DbName,SchemaName,TableName,IndexName,partition,TotalSpaceMb,page_count,frag_count,IndexType,frag_pct,NumPartitions,density from FRG_last $where order by $orderby" 
$req = MSSQLscalar $connstr $q
foreach ($r in $req) {
  if ($global:finishflag -eq 1) {
    Write-host "Stopped by signal from keyboard"
    exit
  }
  if ($global:finishflag -eq 2) {
    Write-host "last index aborted and skipped, continue"
    $global:finishflag = 0
  }
  # sanity check, at this moment no indexes should be in running resumable state
  $runaway = MSSQLscalar $connstrREAL @"
    create table #res (n int, db sysname)
    exec sp_MSforeachdb N'insert into #res select count(*),''?'' from [?].sys.index_resumable_operations where state=0'
    select sum(n) as cnt, max(case when n=0 then '' else db end) as db from #res
"@
  if ($runaway.cnt -gt 0) {
    Write-Host -ForegroundColor Red "Check sys.index_resumable_operations - index rebuild is running (db $($runaway.db))! Runaway? Leaked from this script?"
    exit 
  }

  $db = $r.DbName
  $schema = $r.SchemaName
  $tab = $r.TableName
  $ind = $r.IndexName
  $par = $r.partition
  $numpartitions = $r.NumPartitions
  $siz = $r.TotalSpaceMb
  $pages = $r.page_count
  $frags = $r.frag_count
  $density = $r.density
  $fragpct = $r.frag_pct
  $itype = $r.IndexType
  $totalpct = 100. * $workdone / $worksize
  Write-Host ""
  Write-Host "Next Index size is $siz Mb ..."
  $global:reorg_pct_adder = 0.00
  $global:reorg_elapsed_adder = 0

  if (($workdone -gt $maxdailysize) -and ($maxdailysize -gt 0)) {
    Write-Host "Terminated, processed size so far: $workdone, daily limit: $maxdailysize"
    exit
  }
  if ($deadline -gt "") {
    if ((get-date) -gt $dl) {
      Write-host -ForegroundColor Red "Deadline reached, stopping"
      LOG "" "" "" "" 0 "DEADLINE" $deadline
      exit
    }
  }
  $host.ui.RawUI.WindowTitle = $global:progress + " $workdone Mb of $schema.$tab.$ind ($par)"
  
  Write-Host ""
  $log_throttled = 1
  while ($log_throttled -gt 0) {
    $logused, $logpct, $qlen, $other, $cpupct, $spidcnt = THROTTLER $db
    if ($cpupct -gt $maxcpu) {
      Write-Host -ForegroundColor Yellow "  throttled, waiting cpu < $maxcpu"
      Start-Sleep -Seconds 60      
    }
    elseif (($maxlogused -gt 0) -and ($logused -gt $maxlogused)) {
      Write-Host -ForegroundColor Yellow "  throttled, waiting for logused < $maxlogused Mb"
      Start-Sleep -Seconds 60
    }
    elseif (($maxlogusedpct -gt 0) -and ($logpct -gt $maxlogusedpct)) {
      Write-Host -ForegroundColor Yellow "  throttled, waiting for logusedpct < $maxlogusedpct %"
      Start-Sleep -Seconds 60
    }
    elseif ($qlen -gt $maxqlen / 2) {
      Write-Host -ForegroundColor Yellow "  throttled, waiting for AlwaysOn queue < $($maxqlen/2)"
      Start-Sleep -Seconds 60
    }
    elseif ($other -eq 3) {
      exit
    }
    elseif ($other -gt 0) {
      Write-Host -ForegroundColor Yellow "  throttled, waiting for custom condition"
      Start-Sleep -Seconds 60
    }
    else { $log_throttled = 0 }
  }
  
  if ($totalpct -gt 1.0) {
    $elapsed = (get-date) - $started
    $left = ($elapsed.TotalSeconds / $totalpct) * (100 - $totalpct)
    $eta = (Get-Date).AddSeconds($left).ToString("yyyy-MM-dd HH:mm:ss")
    $totalpct = $totalpct.tostring('###.##', [Globalization.CultureInfo]::CreateSpecificCulture('en-US'))
    $global:progress = "Total progress: ETA: $eta  pct done: $totalpct%"
    Write-Host $global:progress
    $host.ui.RawUI.WindowTitle = $global:progress + " $workdone Mb of $schema.$tab.$ind"
  }

  # $result: 0 OK, 1 KILLED, 2 PAUSED 3 MAXDURATION 4 CANT REBUILD RESUMABLE 5 CANT REBUILD ONLINE
  # strange, but -1 is also OK
  #$dbg = "insert into [$tuning].dbo.FRG_DBG select '$db','$schema','$tab','$ind',$par,$pages,$frags,reads,writes,logical_reads from sys.dm_exec_requests where session_id=@@spid"
  $retrycnt = 0
  if ($reorganize -gt 0) {
    $cmd = "USE [$db]; ALTER INDEX [$ind] on [$schema].[$tab] REORGANIZE "
    if ($numpartitions -gt 1) { $cmd = $cmd + " PARTITION=$par " }
    if ($reorganizeopt -gt "") { $cmd = $cmd + " WITH (" + $reorganizeopt + ")" }
    $op = "REORG"
    $pausing = ""
  }
  elseif (($itype -eq "CLUSTERED COLUMNSTORE") -and ($ent -eq 0)) {
    $cmd = "USE [$db]; ALTER INDEX [$ind] on [$schema].[$tab] REBUILD "
    if ($numpartitions -gt 1) { $cmd = $cmd + " PARTITION=$par " }
    if ($columnstoreopt -gt "") { $cmd = $cmd + " WITH (" + $columnstoreopt + ")" }
    $op = "REBUILD"
    $pausing = ""
  }
  elseif ($itype -eq "CLUSTERED COLUMNSTORE") {
    $cmd = "USE [$db]; ALTER INDEX [$ind] on [$schema].[$tab] REBUILD "
    if ($numpartitions -gt 1) { $cmd = $cmd + " PARTITION=$par " }
    $op = "REBUILD"
    $pausing = "USE [$db]; ALTER INDEX [$ind] on [$schema].[$tab]"
    $cmd = $cmd + " WITH (ONLINE=ON"
    $op = $op + "-OR"
    if ($columnstoreopt -gt "") { $cmd = $cmd + "," + $columnstoreopt } 
    $cmd = $cmd + ")"
  }
  elseif (($ent -eq 0) -and ($itype -eq "HEAP")) {
    $cmd = "USE [$db]; ALTER TABLE [$schema].[$tab] REBUILD "
    if ($numpartitions -gt 1) { $cmd = $cmd + " PARTITION=$par " }
    if (($rebuildopt -gt "") -and ($siz -lt $sortintempdb)) { $cmd = $cmd + " WITH (" + $rebuildopt + ",SORT_IN_TEMPDB=ON)" }
    elseif ($rebuildopt -gt "") { $cmd = $cmd + " WITH (" + $rebuildopt + ")" }
    #$cmd = $cmd + " ; $dbg"
    $op = "REBUILD"    
    $pausing = ""
  }
  elseif ($ent -eq 0) {
    $cmd = "USE [$db]; ALTER INDEX [$ind] on [$schema].[$tab] REBUILD "
    if ($numpartitions -gt 1) { $cmd = $cmd + " PARTITION=$par " }
    if (($rebuildopt -gt "") -and ($siz -lt $sortintempdb)) { $cmd = $cmd + " WITH (" + $rebuildopt + ",SORT_IN_TEMPDB=ON)" }
    elseif ($rebuildopt -gt "") { $cmd = $cmd + " WITH (" + $rebuildopt + ")" }
    #$cmd = $cmd + " ; $dbg"
    $op = "REBUILD"
    $pausing = ""
  }
  elseif ($itype -eq "HEAP") {
    $cmd = "USE [$db]; ALTER TABLE [$schema].[$tab] REBUILD "
    $op = "REBUILD"
    $pausing = "USE [$db]; ALTER INDEX [$ind] on [$schema].[$tab]"
    $cmd = $cmd + " WITH (ONLINE=ON" 
    $op = $op + "-O"
    if ($rebuildopt -gt "") { $cmd = $cmd + "," + $rebuildopt }
    $cmd = $cmd + ")"
  }
  else {
    $cmd = "USE [$db]; ALTER INDEX [$ind] on [$schema].[$tab] REBUILD "
    if ($numpartitions -gt 1) { $cmd = $cmd + " PARTITION=$par " }
    $op = "REBUILD"
    $pausing = "USE [$db]; ALTER INDEX [$ind] on [$schema].[$tab]"
    $cmd = $cmd + " WITH (ONLINE=ON, RESUMABLE=ON"
    $op = $op + "-OR"
    if ($rebuildopt -gt "") { $cmd = $cmd + "," + $rebuildopt }
    $cmd = $cmd + ")"
  }

  $origcmd = $cmd
  while ($True) {
    Write-Host -ForegroundColor Green $cmd
    $xxx = LOG $db $schema $tab $ind $par $op "" # starting

    # this is a typical pwsh problem, value leak. Result should be in $result, but it is poisoned with -1 from somewhere. Use global var
    $global:gresult = -99
    $result = ADEFRAG $connstrREAL $cmd $pausing    $db $schema $tab $ind $par
    $result = $global:gresult
    if ($result -eq 0) {
      $workdone = $workdone + $siz 
      break 
    }
    if ($result -eq 8) {
      # race condition, finished before throttled or killed
      Write-Host("Hmmm, already finished")
      $workdone = $workdone + $siz 
      break 
    }
    if ($result -eq 1) {
      # KILLED
      $retrycnt = $retrycnt + 1
      Write-Host -ForegroundColor Yellow "  wait $relaxation seconds between retries"
      LOG $db $schema $tab $ind $par "WAIT" ""
      Start-Sleep -Seconds $relaxation
      if (-not (($cmd.contains("RESUMABLE=ON") -or $cmd.contains(" RESUME") -or $cmd.contains(" REORGANIZE"))) -and ($retrycnt -gt $offlineretries)) { 
        Write-host -ForegroundColor Red "Giving up after $retrycnt retries"
        break 
      }
      else { continue }
    }
    if (($result -eq 2) -or ($result -eq 3)) {
      # PAUSED
      $retrycnt = $retrycnt + 1
      Write-Host "  wait $relaxation seconds between retries"
      LOG $db $schema $tab $ind $par "WAIT" ""
      Start-Sleep -Seconds $relaxation
      if (-not $cmd.contains(" REORGANIZE")) { $cmd = "USE [$db]; ALTER INDEX [$ind] on [$schema].[$tab] RESUME" }
      continue
    }
    if ($result -eq 6) {
      # THROTTLED
      Write-Host "  wait $relaxation seconds between retries"
      LOG $db $schema $tab $ind $par "WAIT" ""
      Start-Sleep -Seconds $relaxation
      $throttled = 1
      while ($throttled -gt 0) {
        if (($harddeadline -gt 0) -and ($deadline -gt "") -and ((get-date) -gt $dl)) { break } # force exit
        $logused, $logpct, $qlen, $other, $cpupct, $spidcnt = THROTTLER $db
        if ($other -eq 3) {
          # panic
          if (-not $cmd.contains(" REORGANIZE")) {
            $cmd = "USE [$db]; ALTER INDEX [$ind] on [$schema].[$tab] ABORT"
            write-Host -ForegroundColor Yellow $cmd
            $status = MSSQLexec $connstrREAL $cmd
          }
          else { 
            fKILL $connstr $db 1             
          }
          LOG "" "" "" "" 0 "PANIC" ""
          exit
        }
        if ((($maxlogused -gt 0) -and ($logused -gt $maxlogused)) `
            -or (($maxlogusedpct -gt 0) -and ($logpct -gt $maxlogusedpct)) `
            -or ($other -gt 0) `
            -or ($cpupct -gt $maxcpu) `
            -or ($qlen -gt $maxqlen / 2)) {
          if (($reorganize -eq 1) -and ($deadline -gt "")) { # in REORG, we can just abandon an operation
            if ((get-date) -gt $dl) {
              Write-host -ForegroundColor Red "Deadline reached, stopping"
              LOG "" "" "" "" 0 "DEADLINE" $deadline
              exit
            }
          }  
          Write-Host "  still waiting..."
          fKill $connstr $db -1 # DBG
          Start-Sleep -Seconds $relaxation
        }
        else { $throttled = 0 }
      }
      if (-not $cmd.contains(" REORGANIZE")) { $cmd = "USE [$db]; ALTER INDEX [$ind] on [$schema].[$tab] RESUME" }
      continue
    }
    if ($result -eq 3) {
      if ($deadline -gt "") {
        if ((get-date) -gt $dl) {
          Write-host -ForeroundColor Red "  Deadline reached, stopping and aborting index rebuild"
          if (-not $cmd.contains(" REORGANIZE")) {
            $cmd = "USE [$db]; ALTER INDEX [$ind] on [$schema].[$tab] ABORT"
            write-Host -ForegroundColor Yellow $cmd
            $status = MSSQLexec $connstr $cmd
          }
          else {
            fKILL $connstr $db 1 
          }
          LOG "" "" "" "" 0 "DEADLINE" $deadline
          exit
        }
      }
      Write-host -ForegroundColor Yellow "  Resuming index rebuild"
      if (-not $cmd.contains(" REORGANIZE")) { $cmd = "USE [$db]; ALTER INDEX [$ind] on [$schema].[$tab] RESUME" }
      continue
    }
    if ($result -eq 7) { break } # index force skip
    if ($result -eq 4) {
      if ($siz -gt $allowNonResumable * 1000) {
        Write-Host -ForegroundColor Red "Can't rebuild without RESUMABLE=ON and table > $allowNonResumable Gb"
        LOG $db $schema $tab $ind $par "SKIP" "Can't rebuild without RESUMABLE=ON and table > $allowNonResumable Gb"
        break
      }
      else {
        # CANT REBUILD RESUMABLE, downgrade to ONLINE
        $cmd = "USE [$db]; ALTER INDEX [$ind] on [$schema].[$tab] REBUILD"
        $op = "REBUILD-O"
        $cmd = $cmd + " WITH (ONLINE=ON"
        if ($rebuildopt -gt "") { $cmd = $cmd + "," + $rebuildopt }
        $cmd = $cmd + ")"
        continue
      }
    }
    if ($result -eq 5) {
      if ($siz -gt $allowNonOnline * 1000) {
        Write-Host -ForegroundColor Red "Can't rebuild without ONLINE=ON and table > $allowNonOnline Gb"
        LOG $db $schema $tab $ind $par "SKIP" "Can't rebuild without ONLINE=ON and table > $allowNonOnline Gb"
        break
      }
      else {
        # CANT REBUILD ONLINE, downgrade REBUILD OFFLINE
        $cmd = "USE [$db]; ALTER INDEX [$ind] on [$schema].[$tab] REBUILD"
        $op = "REBUILD"
        if ($rebuildopt -gt "") { $cmd = $cmd + " WITH (" + $rebuildopt + ")" }
        continue
      }
    }
    if ($result -eq 99) {
      # PANIC
      Write-host -ForegroundColor Red "Check connections!"
      exit
    }
    break
  }
}
$dtformatted = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
Write-Host "$dtformatted - All done!"
