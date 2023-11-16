# Databricks notebook source
# %run ./logging

# COMMAND ----------

logger = getlogger(__name__)

# COMMAND ----------

# Databricks notebook source
class DeltaFramework_dynamic(DeltaFramework_base):      
  def addOptimizeSchedule(self):
    # currently am statically returning the optimize schedule
    if isinstance(tablesToProcess,list) and len(tablesToProcess) > 0:
      try:
        for table in tablesToProcess:
          query = f""" update {self.metadata} 
            SET optimizeSchedule = '{spark.conf.get("optimizeSchedule", '20 59 23 ? * Sat')}' where dbname = '{self.dbname}' and tblname = '{table}'
            """
          spark.sql(query)
          logger.info(f" Adding Optimized Schedule for {table} in the metadata")
      except Exception as e:
        logger.error(f" Adding Optimized Schedule for {table} in the metadata result into exception {e}")
        
        query = f""" DELETE FROM {self.metadata} 
            where dbname = '{self.dbname}' and tblname = '{table}'
            """
        spark.sql(query)
        logger.info(f" Deleting metadata entry for table {table}")
  
  # def addVaccumSchedule(self):
  #   # currently am statically returning the optimize schedule
  #   if isinstance(tablesToProcess,list) and len(tablesToProcess) > 0:
  #     try:
  #       for table in tablesToProcess:
  #         query = f""" update {self.metadata} 
  #           SET vaccumSchedule = '{spark.conf.get("vaccumSchedule", '20 59 23 ? * Sat')}' where dbname = '{self.dbname}' and tblname = '{table}'
  #           """
  #         spark.sql(query)
  #         logger.info(f" Adding Vaccum Schedule for {table} in the metadata")
  #     except Exception as e:
  #       logger.error(f" Adding Vaccum Schedule for {table} in the metadata result into exception {e}")
  #       query = f""" DELETE FROM {self.metadata} 
  #           where dbname = '{self.dbname}' and tblname = '{table}'
  #           """
  #       spark.sql(query)
  #       logger.info(f" Deleting metadata entry for table {table}")
