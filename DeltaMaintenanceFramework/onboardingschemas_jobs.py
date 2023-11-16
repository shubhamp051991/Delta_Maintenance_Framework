# Databricks notebook source
# %run ./logging

# COMMAND ----------

logger = getlogger(__name__)

# COMMAND ----------

# Databricks notebook source
class DeltaFramework_jobs(DeltaFramework_base):

  def createOptimizeJob(self):
    w = WorkspaceClient()
    
    dbname = self.dbname.replace(".","_")
    for table in tablesToProcess:
      try:
        optimizeSchedule = spark.sql(f""" select optimizeSchedule from {self.metadata} where dbname = '{self.dbname}' and tblname = '{table}'
                                """).collect()[0].optimizeSchedule
        createJob =  w.jobs.create(name=f"OptimizeTest_{dbname}_{table}",
                          tags={'type':'Optimize','database':dbname,'table':table},
                          job_clusters=[jobs.JobCluster(job_cluster_key="MemoryOptimizedCluster_Optimize",
                                                        new_cluster=compute.CreateCluster(spark_version=spark.conf.get("spark_version"),
                                                        autoscale=compute.AutoScale(min_workers=spark.conf.get("min_worker"),max_workers=spark.conf.get("max_worker")),
                                                        node_type_id=spark.conf.get("node_type_id_optimize"),
                                                        driver_node_type_id=spark.conf.get("driver_node_type_id_optimize"),
                                                        runtime_engine=compute.RuntimeEngine('PHOTON'),
                                                        spark_conf={"spark.databricks.delta.rowLevelConcurrencyPreview":"true"})
                                                      ),jobs.JobCluster(job_cluster_key="DriverIntensive-Vaccum",
                                                      new_cluster=compute.CreateCluster(spark_version=spark.conf.get("spark_version"),
                                                      autoscale=compute.AutoScale(min_workers=spark.conf.get("min_worker"),max_workers=spark.conf.get("max_worker")),
                                                      node_type_id=spark.conf.get("node_type_id_vacuum"),
                                                      driver_node_type_id=spark.conf.get("driver_node_type_id_vacuum"))
                                                    )],
                          tasks=[
                            jobs.Task(task_key=f"OptimizeTest_{table}",
                                      description=f" This task will create an Optimize job for the table {table}",
                                      notebook_task=jobs.NotebookTask(notebook_path=spark.conf.get("optimize_notebook_path"),base_parameters = {'dbname':self.dbname,'tblname':table,'metadata':self.metadata}),
                                      email_notifications=jobs.JobEmailNotifications(on_failure=[spark.conf.get("job_notification_dl")]),
                                      job_cluster_key="MemoryOptimizedCluster_Optimize",
                                      max_retries=3,
                                      min_retry_interval_millis=100000,
                                      retry_on_timeout=False
                                      ),jobs.Task(task_key=f"VaccumTest_{table}",
                                    description=f" This task will create an Vaccum job for the table {table}",
                                    notebook_task=jobs.NotebookTask(notebook_path=spark.conf.get("vacuum_notebook_path"),base_parameters = {'dbname':self.dbname,'tblname':table,'metadata':self.metadata}),
                                    email_notifications=jobs.JobEmailNotifications(on_failure=[spark.conf.get("job_notification_dl")]),
                                    job_cluster_key="DriverIntensive-Vaccum",
                                    max_retries=3,
                                    min_retry_interval_millis=100000,
                                    retry_on_timeout=False
                                    )],
                          schedule=jobs.CronSchedule(quartz_cron_expression=optimizeSchedule,timezone_id="PST")
        )
        # update the jobid back to the metadata table
        query = f" update {self.metadata} set jobId = '{createJob.job_id}' where dbname = '{self.dbname}' and tblname = '{table}'"
        spark.sql(query)
      except Exception as e:
        logger.error(f" Error in creating an Optimize/Vacuum job for table {table} with exception {e}")
        query = f""" DELETE FROM {self.metadata} 
            where dbname = '{self.dbname}' and tblname = '{table}'
            """
        spark.sql(query)
        logger.info(f" removing entry of table {table} from the metadata table, the {table} will not be scheduled for Optimize or Vaccum")
      

  # def createVaccumJob(self):
  #   w = WorkspaceClient()
    
  #   dbname = self.dbname.replace(".","_")
  #   for table in tablesToProcess:
  #     try:
  #       vaccumSchedule = spark.sql(f""" select vaccumSchedule from {self.metadata} where dbname = '{self.dbname}' and tblname = '{table}'
  #                               """).collect()[0].vaccumSchedule
  #       createJob =  w.jobs.create(name=f"Vaccum_{dbname}_{table}",
  #                       tags={'type':'Vaccum','database':dbname,'table':table},
  #                       job_clusters=[jobs.JobCluster(job_cluster_key="DriverIntensive-Vaccum",
  #                                                     new_cluster=compute.CreateCluster(spark_version=spark.conf.get("spark_version"),
  #                                                     autoscale=compute.AutoScale(min_workers=spark.conf.get("min_worker"),max_workers=spark.conf.get("max_worker")),
  #                                                     node_type_id=spark.conf.get("node_type_id_vacuum"),
  #                                                     driver_node_type_id=spark.conf.get("driver_node_type_id_vacuum"))
  #                                                   )],
  #                       tasks=[
  #                         jobs.Task(task_key=f"VaccumTest_{table}",
  #                                   description=f" This task will create an Vaccum job for the table {table}",
  #                                   notebook_task=jobs.NotebookTask(notebook_path=spark.conf.get("vacuum_notebook_path"),base_parameters = {'dbname':self.dbname,'tblname':table,'metadata':self.metadata}),
  #                                   email_notifications=jobs.JobEmailNotifications(on_failure=[spark.conf.get("job_notification_dl")]),
  #                                   job_cluster_key="DriverIntensive-Vaccum",
  #                                   max_retries=3,
  #                                   min_retry_interval_millis=100000,
  #                                   retry_on_timeout=False
  #                                   )],
  #                       schedule=jobs.CronSchedule(quartz_cron_expression=vaccumSchedule,timezone_id="PST")
  #     )
  #       # update the jobid back to the metadata table
  #       query = f" update {self.metadata} set vaccumjobid = '{createJob.job_id}' where dbname = '{self.dbname}' and tblname = '{table}'"
  #       spark.sql(query)
  #     except Exception as e:
  #       logger.error(f" Error in creating a Vaccum job for table {table} with exception {e}")
  #       query = f""" DELETE FROM {self.metadata} 
  #           where dbname = '{self.dbname}' and tblname = '{table}'
  #           """
  #       spark.sql(query)
  #       logger.info(f" removing entry of table {table} from the metadata table, the {table} will not be scheduled for Vaccum or Optimize")
