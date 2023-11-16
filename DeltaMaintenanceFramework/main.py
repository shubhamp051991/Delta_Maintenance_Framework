# Databricks notebook source
# MAGIC %pip install --upgrade databricks-sdk

# COMMAND ----------

# MAGIC %pip install databricks-sdk

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Get Parameters
dbname = dbutils.widgets.getArgument("dbname")
tblname = dbutils.widgets.getArgument("tblname")
tblname = tblname.split(",")
metadataTableName = dbutils.widgets.getArgument("metadataTableName")
optimize_notebook_path = dbutils.widgets.getArgument("optimize_notebook_path")
vacuum_notebook_path = dbutils.widgets.getArgument("vacuum_notebook_path")
env = dbutils.widgets.getArgument("env")
cloud = dbutils.widgets.getArgument("cloud")

# COMMAND ----------

# some constraints on the parameters
if len(dbname) == 0:
  raise Exception("Database name must be a valid string")
if len(metadataTableName) == 0:
  raise Exception("Metadata table name must be a valid table name")
if len(optimize_notebook_path) == 0:
  raise Exception("Optimize Notebook Path must be a valid path")
if len(vacuum_notebook_path) == 0:
  raise Exception("Vacuum Notebook Path must be a valid path")
if len(env) == 0:
  env='dev'
if len(cloud) == 0:
  cloud='gcp'

# COMMAND ----------

# spark.sql(f"""create table if not exists {dbname}.{tblname}
# (dbname string not null,tblname string not null,format string,filesize string,sizeGBs string, optimizeSchedule string, optimizeStatus string, lastRunOptimize String, vaccumSchedule String, vaccumStatus string, lastRunVaccum string,optimizejobid string,vaccumjobid string,health_count int,failedAttempts int,successAttempts int)""")

# COMMAND ----------

# MAGIC %md running other notebooks

# COMMAND ----------

# MAGIC %run ./utils $metadataTableName=metadataTableName $optimize_notebook_path=optimize_notebook_path $vacuum_notebook_path=vacuum_notebook_path $env=env $cloud=cloud

# COMMAND ----------

# MAGIC %run ./onboardingschemas

# COMMAND ----------

# MAGIC %run ./onboardingschemas_dynamic

# COMMAND ----------

# MAGIC %run ./onboardingschemas_jobs

# COMMAND ----------

if __name__ == '__main__':

  schema = DeltaFramework_base(dbname=f"{dbname}")
  if len(tblname) > 0:
    staticresult = schema.addingStaticFields(tblname)
  else:
    staticresult = schema.addingStaticFields()

  schema_dynamic = DeltaFramework_dynamic(dbname=f"{dbname}")
  dynamicoptimizeresult = schema_dynamic.addOptimizeSchedule()



  schema_jobs = DeltaFramework_jobs(dbname=f"{dbname}")
  jobscreateresult = schema_jobs.createOptimizeJob()
  # jobscreateresult = schema_jobs.createVaccumJob()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Notes
# MAGIC * User can add Database and all eligible tables will be processed
# MAGIC * User can add a specific table and only that table will be processed
# MAGIC   * if there is any error in creating either optimized or vacuum job for a given table, then all operation will rollback
# MAGIC * Row level concurrency will be enabled on the job clusters running Optimize - resolve conflict by design
# MAGIC   * DBR > 14.0 + Photon will be required for running Optimize with row level concurrency enabled [Public Preview]
# MAGIC   * Minimum DBR version needed is 12.3LTS and tables will have to be deletion vector enabled
# MAGIC   * Streaming tables with Continous mode will not be enabled by default
# MAGIC   * if any streaming table is used by dbsql or DLT then that has to me removed as well, limitations of deletion vector
# MAGIC * After every 10 iterations of Optimize, a filesize will be re-evaluated and targetFileSize will be updated based on that
# MAGIC * Metadata will capture last success and last failure instance and will set an alert if any failure entry has been written

# COMMAND ----------

# MAGIC %md
# MAGIC * Test on GCP
# MAGIC * Config files for env specific [YAML]
