USE [tempdb]
GO
drop TABLE [dbo].[FRG_SizeStats]
GO
DROP TABLE [dbo].[FRG_Levels]
GO
drop TABLE [dbo].[FRG_LOG]
GO
drop TABLE FRG_UsageStats
GO
drop view [dbo].[FRG_levelsLast]
GO
drop view [dbo].[FRG_SizeStatsLast]
GO
drop view [dbo].[FRG_last]
GO
drop procedure [dbo].[FRG_FillSizeStats]
GO
drop procedure [dbo].[FRG_FillFragmentation]
GO
drop procedure [dbo].[FRG_FillFragmentationOne]
GO
drop procedure [dbo].[FRG_FillUsageStats]
GO
drop procedure [dbo].FRG_PrintUsageStats
GO