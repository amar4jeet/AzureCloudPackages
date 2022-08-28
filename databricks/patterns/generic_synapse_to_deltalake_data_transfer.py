# Databricks notebook source
#------------------------------------------------------
# Imports
#------------------------------------------------------
import zlib
from base64 import b64encode, b64decode
import json
from delta.tables import *
from pyspark.sql.functions import col, hash, sha1, lit, concat, concat_ws
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
import numpy as np

# COMMAND ----------

#Parameterizing the variables to be used in the script
dbutils.widgets.text('LogicalEnv','tst002')
LogicalEnv=dbutils.widgets.get('LogicalEnv').strip()
dbutils.widgets.text('source_schema','dimlayer')
source_schema=dbutils.widgets.get('source_schema').strip()
dbutils.widgets.text('target_schema','transform')
target_schema=dbutils.widgets.get('target_schema').strip()
dbutils.widgets.text('table','dim_connection_point_device')
table=dbutils.widgets.get('table').strip()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------------------------
# Below parameters retrieved from databrics secret scope linked to key vault
# Authentication with service principals is not supported for loading data into and unloading data from Azure Synapse.
#----------------------------------------------------------------------------------------------------------------------
 
jdbcKeyName = "imdw-jdbc-large-connstring"
#prepare connection string for sqldw
sqldwConnstring = dbutils.secrets.get(scope = "dfstore", key=jdbcKeyName)
print(sqldwConnstring)


# COMMAND ----------

table_name = "im" + LogicalEnv + "_" + source_schema + "." + table
jdbcDF = spark.read.format("jdbc") \
.option("url", sqldwConnstring) \
.option("dbtable", table_name) \
.option('driver', 'com.microsoft.sqlserver.jdbc.SQLServerDriver')\
.load()
#display(jdbcDF)
jdbcDF.createOrReplaceTempView("jdbcDFtable")

# COMMAND ----------


droppTableName="im" + LogicalEnv + "_" + target_schema + "." + table
#print(droppTableName)
sql_qry='drop table if exists '+ droppTableName
#print(sql_qry)
spark.sql(sql_qry)


# COMMAND ----------


deleteFilelocation="/mnt//imrv/deltalake/im" + LogicalEnv + "/" + target_schema +"/" + table;
for src_path in dbutils.fs.ls(deleteFilelocation):
  print(src_path)
  dbutils.fs.rm(src_path.path, True)

# COMMAND ----------

createTableName="im" + LogicalEnv + "_" + target_schema + "." + table
createTablelocation="/mnt//imrv/deltalake/im" + LogicalEnv + "/" + target_schema +"/" + table

create_tbl_stmnt='''
  CREATE TABLE {0}
  USING DELTA
  LOCATION {1}
  as
  With qry AS
  (
  SELECT * FROM jdbcDFtable
  )
  select * from qry
  '''.format(createTableName,repr(createTablelocation))
spark.sql(create_tbl_stmnt)
