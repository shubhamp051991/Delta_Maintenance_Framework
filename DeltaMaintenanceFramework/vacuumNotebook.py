# Databricks notebook source
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

logger = getlogger(__name__)

# COMMAND ----------

dbname = dbutils.widgets.getArgument("dbname")
tblname = dbutils.widgets.getArgument("tblname")
metadata = dbutils.widgets.getArgument("metadata")

# COMMAND ----------

try:
  spark.sql(f""" vacuum {dbname}.{tblname} retain 720 hours""")
except Exception as e:
  logger.exception(f" Exception running vaccum on {dbname}.{tblname} as {e}")
