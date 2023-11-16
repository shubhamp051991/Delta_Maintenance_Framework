# Databricks notebook source
metadataTableName = dbutils.widgets.getArgument("metadataTableName")
optimize_notebook_path = dbutils.widgets.getArgument("optimize_notebook_path")
vacuum_notebook_path = dbutils.widgets.getArgument("vacuum_notebook_path")
env = dbutils.widgets.getArgument("env")
cloud = dbutils.widgets.getArgument("cloud")

# COMMAND ----------

# settings default arguments
optimizeSchedule = '20 59 23 ? * Sat'
vaccumSchedule = '20 59 23 ? * Sat'

# COMMAND ----------

import logging
import pyspark.sql.functions as f
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
from databricks.sdk.service import compute
tablesToProcess = []

# COMMAND ----------

import logging
def getlogger(name, level=logging.INFO):
    import logging
    import sys

    logger = logging.getLogger(name)
    logger.setLevel(level)
    if logger.handlers:
        pass
    else:
        ch = logging.StreamHandler(sys.stderr)
        ch.setLevel(level)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)
    return logger

# COMMAND ----------

def set_permission(env:str):
  # setting permissions as per the env
  if env.lower() == "prod":
    spark.conf.set("metadataTableName", metadataTableName)
    spark.conf.set("optimizeSchedule", optimizeSchedule)
    spark.conf.set("vaccumSchedule", vaccumSchedule)
    spark.conf.set("optimize_notebook_path", optimize_notebook_path)
    spark.conf.set("vacuum_notebook_path", vacuum_notebook_path)
    
  elif env.lower() == "qa":
    spark.conf.set("metadataTableName", metadataTableName)
    spark.conf.set("optimizeSchedule", optimizeSchedule)
    spark.conf.set("vaccumSchedule", vaccumSchedule)
    spark.conf.set("optimize_notebook_path", optimize_notebook_path)
    spark.conf.set("vacuum_notebook_path", vacuum_notebook_path)
    
  elif env.lower() == "dev":
    spark.conf.set("metadataTableName", metadataTableName)
    spark.conf.set("optimizeSchedule", optimizeSchedule)
    spark.conf.set("vaccumSchedule", vaccumSchedule)
    spark.conf.set("optimize_notebook_path", optimize_notebook_path)
    spark.conf.set("vacuum_notebook_path", vacuum_notebook_path)

  else:
    raise Exception("Unsupported Environment value")

# COMMAND ----------

def setConfiguration(metadataTableName:str,env:str,cloud:str):
  spark.conf.set("spark_version", "14.0.x-scala2.12") # set this to >14.0 always as we need Row level concurreny to be enabled
  spark.conf.set("min_worker", 1)
  spark.conf.set("max_worker", 5)
  spark.conf.set("job_notification_dl", 'abc@abc.com')
  if cloud.lower() == 'gcp':
    spark.conf.set("node_type_id_optimize", "n2-highmem-4")
    spark.conf.set("driver_node_type_id_optimize", "n2-highmem-4")
    spark.conf.set("node_type_id_vacuum", "n1-standard-4")
    spark.conf.set("driver_node_type_id_vacuum", "n1-standard-8")
  elif cloud.lower() == 'aws':
    spark.conf.set("node_type_id_optimize", "r5d.large")
    spark.conf.set("driver_node_type_id_optimize", "r5d.large")
    spark.conf.set("node_type_id_vacuum", "i3.xlarge")
    spark.conf.set("driver_node_type_id_vacuum", "i3.2xlarge")
  elif cloud.lower() == 'azure':
    spark.conf.set("node_type_id_optimize", "Standard_DS12_v2")
    spark.conf.set("driver_node_type_id_optimize", "Standard_DS12_v2")
    spark.conf.set("node_type_id_vacuum", "Standard_DS3_v2")
    spark.conf.set("driver_node_type_id_vacuum", "Standard_DS4_v2")
  else:
    # add logger for the default cloud argument
    pass

  # setting permissions for the specific env
  set_permission(env)

# COMMAND ----------

setConfiguration(metadataTableName,env,cloud)
