# Databricks notebook source
# dbutils.widgets.text("dbname","")
# dbutils.widgets.text("tblname","")
dbname = dbutils.widgets.getArgument("dbname")
tblname = dbutils.widgets.getArgument("tblname")
metadata = dbutils.widgets.getArgument("metadata")

# COMMAND ----------

try:
  query = f" OPTIMIZE {dbname}.{tblname}"
  spark.sql(query)

  query = f""" update {metadata}
      set optimizeStatus = 'Success',
      lastRunOptimize = current_timestamp(), 
      health_count =    ifnull(health_count,0)+1,
      failedAttempts = 0,
      successAttempts = ifnull(successAttempts,0)+1
      where dbname = '{dbname}' and tblname = '{tblname}'
  """
  spark.sql(query)
except Exception as e:
  query = f""" update {metadata}
      set optimizeStatus = 'Failed',
      lastRunOptimize = current_timestamp(),
      health_count = ifnull(health_count,0)+1,
      successAttempts = 0,
      failedAttempts = ifnull(failedAttempts,0)+1
      where dbname = '{dbname}' and tblname = '{tblname}'
  """
  spark.sql(query)

# COMMAND ----------

def findFileSize(sizeOfTable):
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

def _addToMetadata(dbname,tbl):
  try:
    tableRow = spark.sql(f""" describe detail {dbname}.{tbl}
              """).select("format","numFiles","sizeInBytes").collect()[0]
    format = tableRow.format
    numFiles = int(tableRow.numFiles)
    sizegb = round((tableRow.sizeInBytes*1.0)/1024/1024/1024,0)
    filesize = findFileSize(sizegb)
    
    query = f""" Alter table {dbname}.{tblname} SET TBLPROPERTIES('delta.targetFileSize'='{filesize}') """
    spark.sql(query)
    
    # updating the metadata table with the sizegbs
    query = f""" update {metadata} 
    set sizeGBs = '{sizegb}' , filesize = '{filesize}' where dbname = '{dbname}' and tblname = '{tblname}'
    """
    spark.sql(query)
  except Exception as e:
    print(e)
    return False

# COMMAND ----------

# get the health_count from the metadata table and if it's value  = 10, then we reevaluate the filesize and sizegbs
health_count = spark.sql(f" select health_count from {metadata} where dbname = '{dbname}' and tblname = '{tblname}' ").collect()[0].health_count

if health_count > 10:
  _addToMetadata(dbname,tblname)

  # also reset the health count to 0
  query = f" update {metadata} set health_count = 0 where dbname = '{dbname}' and tblname = '{tblname}'"
  # logger
  spark.sql(query)

# COMMAND ----------


