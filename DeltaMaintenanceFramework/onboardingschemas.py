# Databricks notebook source
# %run ./logging

# COMMAND ----------

logger = getlogger(__name__)

# COMMAND ----------

# Databricks notebook source
class DeltaFramework_base():
  def __init__(self,dbname,tblname=None):
    self.dbname = dbname
    self.metadata = spark.conf.get("metadataTableName")

  def _getSchema(self):
    return self.dbname
  
  # check if table already exists
  def _tblexists(self,tbl):
    if spark.sql(f""" select count(1) from {self.metadata} where dbname = '{self.dbname}' and tblname = '{tbl}' """).limit(1).take(1)[0][0] > 0:
      return True
    else:
      return False
  
  # checks if database already exists
  def _dbexists(self):
    if spark.sql(f""" select count(1) from {self.metadata} where dbname = '{self.dbname}' """).limit(1).take(1)[0][0] >= 0:
      tablesRow = spark.sql(f"show tables in {self.dbname} ").select("tableName").collect()
      tables = [tbl[0] for tbl in tablesRow]
      for tbl in tables:
        if self._tblexists(tbl) == False and spark.sql(f"""describe extended {self.dbname}.{tbl}""").filter(f.col("col_name") == "Type").select("data_type").collect()[0].data_type != "VIEW" and spark.sql("describe extended nmishra_catalog.circana_2.catalog_privileges_table").filter(f.col("col_name") == "Provider").select("data_type").collect()[0].data_type == 'delta':
          tablesToProcess.append(tbl)
        else:
          logger.info(f" Table {tbl} is a view or not a delta format or the table already exist in the metadata table")
      if len(tablesToProcess) > 0:
        logger.info(f" Tables to process are {tablesToProcess}")
        
  def _isStreamingTable(self,tbl):
    if len(spark.sql(f"desc history {self.dbname}.{tbl}").filter(f.col("operation").contains("STREAMING")).sort(f.col("timestamp").desc()).limit(1).take(1)) > 0:
      return True
    return False

  def findFileSize(self,sizeOfTable):
    if sizeOfTable <= 0:
      return '64mb'
    elif sizeOfTable > 0 and sizeOfTable <= 1000:
      return '128mb'
    elif sizeOfTable > 1000 and sizeOfTable <= 2560:
      return '256mb'
    elif sizeOfTable > 2560 and sizeOfTable <= 3000:
      return '328mb'
    elif  sizeOfTable > 3000 and sizeOfTable <= 5000:
      return '512mb'
    elif  sizeOfTable > 5000 and sizeOfTable <= 7000:
      return '728mb'
    else:
      return '1gb'

  def _addToMetadata(self,tbl):
    try:
      tableRow = spark.sql(f""" describe detail {self.dbname}.{tbl}
                """).select("format","sizeInBytes").collect()[0]
      format = tableRow.format
      sizegb = round((tableRow.sizeInBytes*1.0)/1024/1024/1024,0)
      filesize = self.findFileSize(sizegb)
      streaming = self._isStreamingTable(tbl)

      if streaming:
        query = f""" Alter table {self.dbname}.{tbl} SET TBLPROPERTIES('delta.targetFileSize'='{filesize}','delta.enableDeletionVectors'='true') """
        spark.sql(query)
      else:
        query = f""" Alter table {self.dbname}.{tbl} SET TBLPROPERTIES('delta.targetFileSize'='{filesize}','delta.autoOptimize.autoCompact'='auto','delta.autoOptimize.optimizeWrite'='true','delta.enableDeletionVectors'='true') """
        spark.sql(query)

      query = f""" INSERT INTO {self.metadata} (dbname,tblname,format,filesize,sizeGBs)
      SELECT '{self.dbname}','{tbl}','{format}','{filesize}','{sizegb}'
      """
      spark.sql(query)
      logger.info(f" Adding {tbl} static fields into the metadata table")
    except Exception as e:
      # logger.exception(f" {e}")
      logger.warning(f"Exception occured for table {tbl}")
      return False
    
  def _processTables(self):
    if isinstance(tablesToProcess,list):
      for tables in tablesToProcess:
        result = self._addToMetadata(tables)
        if result == False:
          pass
        else:
          logger.info(f" Static information for table {tables} added")

  # this function will utilize the list from _dbexists and add the static fields to the metadata table
  def addingStaticFields(self,tblname:str = None):
    # check if explicit table list is provided
    if tblname[0] != "":
      logger.info(f" Explicit tablename string is provided, complete database {self.dbname} will not be scanned")
      for table in tblname:
        if self._tblexists(table):
          pass
        else:
          tablesToProcess.append(table)
        logger.info(f" Tables to process are {tablesToProcess}")
      if len(tablesToProcess) == 0:
        logger.error(f"  No tables to process, Please pass tables which are not in the metadata or are explicit tables, we don't process views")
        raise Exception(f"  No specific table is eligible to process")
      self._processTables()
    else:
      logger.info(f" complete database {self.dbname} will be scanned for tables to process")
      self._dbexists()
      if len(tablesToProcess) == 0:
        logger.error(f"  No tables to process, Please pass tables which are not in the metadata or are explicit tables, we don't process views")
        raise Exception(f"  No specific table is eligible to process")
      self._processTables()
