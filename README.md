GentleRebuild is Powershell script for online index rebuild in high-load clustered MSSQL databases working 24/7
nterprise Edition is expected, Standard Edition is not supported. 

Script executes ALTER INDEX ... REBUILD in a safe controlled mode using 2 threads: 
  one thread is executing the command while second one is checking the environment for several conditions.

for more information, check usageGuide or https://www.actionatdistance.com/GentleRebuild.html

