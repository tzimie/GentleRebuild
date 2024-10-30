USE [DBAdefrag] 
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[FRG_SizeStats](
	[DT] [datetime] NOT NULL,
	[DBID] [smallint] NULL,
	[DbName] [nvarchar](128) NULL,
	[SchemaName] [sysname] NULL,
	[TableName] [sysname] NOT NULL,
	[IndexName] [sysname] NOT NULL,
	[IndexType] [nvarchar](60) NOT NULL,
    [FileGroupName] sysname,
	[table_id] [int] NOT NULL,
	[index_id] [int] NOT NULL,
	[rows] [bigint] NULL,
	[partition] [int] NULL,
	[TotalSpaceMB] [bigint] NULL,
	compression varchar(120) null,
	seeks float, scans float, activity float
) ON [PRIMARY]
GO

create table FRG_UsageStats (
  [DbName] [nvarchar](128) NULL,
  table_id int,
  index_id int,
  [partition] int,
  seeks float, scans float, activity float)
GO

CREATE TABLE [dbo].[FRG_Levels](
	[DT] [datetime] NULL,
	[db_id] [int] NULL,
	[table_id] [int] NULL,
	[index_id] [int] NULL,
	[partition] [int] NULL,
	[atype] [varchar](32) NULL,
	[fragment_count] [int] NULL,
	[frag_pct] [float] NULL,
	[page_used_pct] [float] NULL,
	[page_count] [int] NULL,
	[frag_size_pages] [float] NULL,
	[density] [float] NULL,
	[depth] [int] NULL
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[FRG_LOG](
	[n] [int] IDENTITY(1,1) NOT NULL,
	[DT] [datetime] NULL,
	[DbName] [sysname] NOT NULL,
	[SchemaName] [sysname] NOT NULL,
	[TableName] [sysname] NOT NULL,
	[IndexName] [sysname] NOT NULL,
	[Partition] [int] NULL,
	[Op] [varchar](32) NULL,
	[Message] [varchar](max) NULL,
PRIMARY KEY CLUSTERED 
(
	[n] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

ALTER TABLE [dbo].[FRG_LOG] ADD  DEFAULT (getdate()) FOR [DT]
GO

CREATE view [dbo].[FRG_levelsLast]
as
  select DT,db_id,table_id,index_id,partition,atype,fragment_count,frag_pct,page_used_pct,page_count,frag_size_pages,density,depth 
    from (
      select DT,db_id,table_id,index_id,partition,atype,fragment_count,frag_pct,page_used_pct,page_count,frag_size_pages,density,depth
      ,(row_number() over (partition by db_id,table_id,index_id,partition order by DT desc)) as ord
      from FRG_levels) Q
	  where ord=1 -- last
GO

CREATE view [dbo].[FRG_SizeStatsLast]
as
  select DT,dbid,DBname,SchemaName,TableName,IndexName,Partition,IndexType,FilegroupName,table_id,index_id,rows,TotalSpaceMb,compression,seeks,scans,activity
    from (
      select DT,dbid,DBname,SchemaName,TableName,IndexName,Partition,IndexType,FilegroupName,table_id,index_id,rows,TotalSpaceMb,compression,seeks,scans,activity
      ,(row_number() over (partition by dbid,table_id,index_id,partition order by DT desc)) as ord
      from FRG_SizeStats) Q
	  where ord=1 -- last
GO

create view [dbo].[FRG_last]
as
select F.DT,Dbname,SchemaName,TableName,IndexName,S.Partition,IndexType,FilegroupName,rows,TotalSpaceMb,compression,
   page_count,isnull(fragment_count,0) as frag_count,
   case when page_count>0 then 100.*fragment_count/page_count else 0 end as frag_pct, frag_pct as frag_pct_sql,
   page_used_pct,isnull(frag_size_pages,0) as frag_size_pages, density, depth, atype
   ,(select count(*) from FRG_levelsLast FF 
      where S.DBID=FF.db_id and S.table_id=FF.table_id and S.index_id=FF.index_id) as NumPartitions
   ,seeks, scans, activity
  from FRG_SizeStatsLast S
  LEFT OUTER JOIN FRG_levelsLast F 
    on S.DBID=F.db_id and S.table_id=F.table_id and S.index_id=F.index_id and S.partition=F.partition
GO

create procedure [dbo].[FRG_FillUsageStats]
  @dbscope varchar(128)=NULL
as
create table #sizes ([DbName] [nvarchar](128), table_id int, index_id int, [rows] bigint, partition int, pages bigint)
create table #usage ([DbName] [nvarchar](128), object_id int, index_id int, seeks bigint, scans bigint)
declare @db nvarchar(256), @sql varchar(8000)
set @db=db_name()
set @sql='use [?]; insert into #sizes
select DB_NAME() as DbName, 
  t.object_id as table_id, i.index_id, rows,
  partition_number as partition, 
  total_pages as pages
  FROM sys.tables t
  INNER JOIN sys.indexes i ON t.OBJECT_ID = i.object_id
  INNER JOIN sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
  INNER JOIN sys.allocation_units a ON partition_id = a.container_id
  WHERE t.is_ms_shipped = 0 AND i.OBJECT_ID > 255 and db_id()>4
insert into #usage select DB_NAME() as DbName, object_id, index_id, user_seeks+user_lookups, user_scans
  from sys.dm_db_index_usage_stats'
if @dbscope is NULL
  EXECUTE master.sys.sp_MSforeachdb @sql
else  
  begin
  set @sql=replace(@sql, '[?]', '['+@dbscope+']')
  exec(@sql)
  end
select DbName,table_id,index_id,sum(rows) as totalrows,sum(pages) AS totalpages 
  into #totals
  from #sizes group by DbName,table_id,index_id
select S.DbName,S.table_id,S.index_id,S.partition,pages,
  case when totalpages=0 then 1.0 else convert(float,pages)/totalpages end as pct
  into #pct
  from #sizes S 
  inner join #totals T on S.DbName=T.DbName and S.table_id=T.table_id and S.index_id=T.index_id
select p.DbName,p.table_id,p.index_id,partition,
  seeks*pct as seeks, scans*pct as scans, (seeks+scans*pages)*pct as activity
  into #final
  from #pct p
  inner join #usage u on p.DbName=u.DbName and p.table_id=u.object_id and p.index_id=u.index_id
truncate table FRG_UsageStats
insert into FRG_UsageStats select * from #final where activity>0
GO

create procedure FRG_PrintUsageStats
as
  select 'truncate table FRG_UsageStats' as txt
  union all
  select 'insert into FRG_UsageStats select '''+DbName+''','
    +convert(varchar,table_id)
    +','+convert(varchar,index_id)
    +','+convert(varchar,partition)
    +','+convert(varchar,seeks)
    +','+convert(varchar,scans)
    +','+convert(varchar,activity)
    from FRG_UsageStats
GO

create procedure [dbo].[FRG_FillSizeStats]
  @dbscope varchar(128)=NULL
as
declare @db nvarchar(256), @sql varchar(8000)
set @db=db_name()
set @sql='use [?];
insert into ['+@db+'].dbo.FRG_SizeStats
select X.*,isnull(U.seeks,0.0),isnull(U.scans,0.0),isnull(U.activity,0.0) from (
 select getdate() as DT, DB_ID() as DBID, DB_NAME() as DbName, 
  SchemaName, TableName, IndexName, IndexType, FilegroupName, object_id as table_id, index_id,
  sum(rows) as rows,
  partition_number as partition, 
  sum(a.total_pages / 128)  AS TotalSpaceMB,
  data_compression_desc as compression
  from (
    SELECT t.object_id, i.index_id,
    s.Name AS SchemaName,
    t.NAME AS TableName,
	isnull(i.name,'''') AS IndexName,
	case when t.is_memory_optimized>0 then ''INMEM:'' else '''' end + isnull(i.type_desc,'''') as IndexType,
    f.name as FilegroupName,
    p.rows,
	p.partition_number, p.partition_id, data_compression_desc
  FROM sys.tables t
  INNER JOIN sys.indexes i ON t.OBJECT_ID = i.object_id
  INNER JOIN sys.filegroups f on f.data_space_id=i.data_space_id
  INNER JOIN sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
  LEFT OUTER JOIN sys.schemas s ON t.schema_id = s.schema_id
  WHERE t.is_ms_shipped = 0 AND i.OBJECT_ID > 255 and db_id()>4
  ) Q
  INNER JOIN sys.allocation_units a ON Q.partition_id = a.container_id
  GROUP BY SchemaName, TableName, IndexName, IndexType, FilegroupName, object_id, index_id, partition_number, data_compression_desc) X
left outer join ['+@db+'].dbo.FRG_UsageStats U on U.DbName=X.DbName and U.table_id=X.table_id and U.index_id=X.index_id and U.partition=X.partition'
if @dbscope is NULL
  EXECUTE master.sys.sp_MSforeachdb @sql
else  
  begin
  set @sql=replace(@sql, '[?]', '['+@dbscope+']')
  exec(@sql)
  end
GO

create procedure [dbo].[FRG_FillFragmentation]
  @dbscope varchar(128)=NULL,
  @mode varchar(128) = 'DETAILED' -- LIMITED, SAMPLED
as
  set nocount on
  declare @dbid int, @dbname sysname, @schemaname sysname, @tablename sysname, @indexname sysname, @tid int, @iid int, @par int
  declare @sql nvarchar(1000)
  DECLARE ti CURSOR FOR SELECT DBID,DbName,SchemaName,TableName,IndexName,table_id,index_id,partition
    from FRG_SizeStatsLast
	where (dbname=@dbscope or @dbscope is null) and IndexType not like 'INMEM%'
	order by TotalSpaceMB 
  OPEN ti
  FETCH NEXT FROM ti INTO @dbid,@dbname,@schemaname,@tablename,@indexname,@tid,@iid,@par
  WHILE @@FETCH_STATUS = 0  
    BEGIN  
	set @sql='Working on ' + @dbname+'.'+@schemaname+'.'+@tablename+' index '+@indexname+' partition '+convert(varchar,@par)
	RAISERROR (@sql, 0, 1) WITH NOWAIT
	set @sql='
insert into ['+db_name()+'].dbo.FRG_Levels 
select getdate(),database_id,object_id,index_id,partition,'''+@mode+''',
  fragment_count,avg_fragmentation_in_percent,avg_page_space_used_in_percent,page_count,avg_fragment_size_in_pages,isnull(density,70) as density,depth 
  from (select database_id,object_id,index_id,partition_number as partition,
        sum(fragment_count) as fragment_count,
        avg(avg_fragmentation_in_percent) as avg_fragmentation_in_percent,
		avg(avg_page_space_used_in_percent) as avg_page_space_used_in_percent,
		sum(page_count) as page_count,
		avg(avg_fragment_size_in_pages) as avg_fragment_size_in_pages,
		sum(page_count*avg_page_space_used_in_percent)/(1+sum(page_count)) as density,
		max(index_level)+1 as depth
        from sys.dm_db_index_physical_stats('+convert(varchar,@dbid)+','+convert(varchar,@tid)+','+convert(varchar,@iid)+','+convert(varchar,@par)+', '''+@mode+''')
		group by database_id,object_id,index_id,partition_number
		) Q '
	--print @sql
	exec(@sql)
    FETCH NEXT FROM ti INTO @dbid,@dbname,@schemaname,@tablename,@indexname,@tid,@iid,@par
    END 
  CLOSE ti
  DEALLOCATE ti
GO

create procedure [dbo].[FRG_FillFragmentationOne] 
  @db sysname, @schemaname sysname, @tablename sysname, @indexname sysname, @par int
as
  set nocount on
  declare @before int, @after int, @mode varchar(128)
  select @before=TotalSpaceMb,@mode=atype from FRG_last where Dbname=@db 
    and Schemaname=@schemaname and TableName=@tablename and IndexName=@indexname and Partition=@par
  declare @sql nvarchar(4000)
  set @sql='use ['+@db+']; '
  set @sql=@sql+'declare @tid int, @iid int '
  set @sql=@sql+' select @tid=object_id(''['+@schemaname+'].['+@tablename+']'') '
  set @sql=@sql+' select @iid=index_id from sys.indexes where object_id=@tid and name='''+@indexname+''''
  set @sql=@sql+' if @iid is null set @iid=0 '
  set @sql=@sql+'
  insert into ['+db_name()+'].dbo.FRG_Levels 
  select getdate(),database_id,@tid,@iid,partition,'''+@mode+''',
  fragment_count,avg_fragmentation_in_percent,avg_page_space_used_in_percent,page_count,avg_fragment_size_in_pages,isnull(density,70) as density,depth 
  from (select database_id,object_id,index_id,partition_number as partition,
        sum(fragment_count) as fragment_count,
        avg(avg_fragmentation_in_percent) as avg_fragmentation_in_percent,
		avg(avg_page_space_used_in_percent) as avg_page_space_used_in_percent,
		sum(page_count) as page_count,
		avg(avg_fragment_size_in_pages) as avg_fragment_size_in_pages,
		sum(page_count*avg_page_space_used_in_percent)/(1+sum(page_count)) as density,
		max(index_level)+1 as depth
        from sys.dm_db_index_physical_stats(db_id('''+@db+'''),@tid,@iid,'+convert(varchar,@par)+','''+@mode+''')
		group by database_id,object_id,index_id,partition_number
		) Q '
  exec (@sql)
  set @sql=
'USE ['+@db+']; 
insert into ['+db_name()+'].dbo.FRG_SizeStats
select X.*,isnull(U.seeks,0.0),isnull(U.scans,0.0),isnull(U.activity,0.0) from (
 select getdate() as DT, DB_ID() as DBID, DB_NAME() as DbName, 
  SchemaName, TableName, IndexName, IndexType, FilegroupName, object_id as table_id, index_id,
  sum(rows) as rows,
  partition_number as partition, 
  sum(a.total_pages / 128)  AS TotalSpaceMB,
  data_compression_desc as compression
  from (
    SELECT t.object_id, i.index_id,
    s.Name AS SchemaName,
    t.NAME AS TableName,
	isnull(i.name,'''') AS IndexName,
	isnull(i.type_desc,'''') as IndexType,
    f.name as FilegroupName,
    p.rows,
	p.partition_number, p.partition_id, data_compression_desc
  FROM sys.tables t
  INNER JOIN sys.indexes i ON t.OBJECT_ID = i.object_id
  INNER JOIN sys.filegroups f on f.data_space_id=i.data_space_id
  INNER JOIN sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
  LEFT OUTER JOIN sys.schemas s ON t.schema_id = s.schema_id
  WHERE t.is_ms_shipped = 0 AND i.OBJECT_ID > 255 and db_id()>4 
  ) Q
  INNER JOIN sys.allocation_units a ON Q.partition_id = a.container_id
  where SchemaName='''+@SchemaName+''' and TableName='''+@TableName+''' and IndexName='''+@IndexName+'''
    and partition_number='+convert(varchar,@par)+' 
  GROUP BY SchemaName, TableName, IndexName, IndexType, FilegroupName, object_id, index_id, partition_number, data_compression_desc) X
left outer join ['+db_name()+'].dbo.FRG_UsageStats U on U.DbName=X.DbName and U.table_id=X.table_id and U.index_id=X.index_id and U.partition=X.partition'
  exec (@sql)
  select @after=TotalSpaceMb from FRG_last where Dbname=@db 
    and Schemaname=@schemaname and TableName=@tablename and IndexName=@indexname and Partition=@par
  select @before as [Before],@after as [After], @before-@after as delta
GO