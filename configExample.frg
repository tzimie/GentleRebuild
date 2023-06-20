{
  server: "bi-nastro-db04",
  dbname: "BankOperation,Offers_DWH",

  allowNonResumable: 50,
  allowNonOnline: 10,
  threshold: 40,
  extrafilter: " and SchemaName not in ('tmp','import') and page_count>100 and DT<getdate()-2 and TotalSpaceMb<1000000",

  deadline: "23:00",
  rebuildopt: "DATA_COMPRESSION=PAGE,MAXDOP=4",
  columnstoreopt: "MAXDOP=1",
  relaxation: 30,
  maxlogused: 200000,
  maxlogusedpct: 0,
  maxqlen: 25000,
  maxdailysize: 1000000,
  chunkminutes: 0,
  killnonresumable: 0,
  workdb: "Tuning"
}