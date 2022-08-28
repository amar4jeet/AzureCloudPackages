# Databricks notebook source
# MAGIC %md
# MAGIC <h5>Script Flow and Steps</h5>
# MAGIC <h6>Following are the list of things that are executed in the script</h6>
# MAGIC <ul>
# MAGIC <li>Import the libraries</li>
# MAGIC <li>Remove the widgets from the earlier execution</li>
# MAGIC <li>Create the widgets for Source table, Destination table and environment</li>
# MAGIC   <ul>
# MAGIC   <li>Source table is the one from where data is copied from, source is Historical table</li>
# MAGIC   <li>Destination table is the one to where data is copied to, dest is the daily load table</li>
# MAGIC   </ul>
# MAGIC   <li>Take the backup of the destination table</li>
# MAGIC   <li>Take the backup of the source table</li>
# MAGIC   <li>Take the count from the deestination and source tables</li>
# MAGIC   <li>Insert the records from the source table to destination table</li>
# MAGIC   <li>Get the destination count and compare with previous count</li>
# MAGIC   <li>If count matches proceed the execution otherwise exit the script</li>
# MAGIC   <li>Delete the Source table and recreate it</li>
# MAGIC </ul>
# MAGIC 
# MAGIC <b>Please replace the DDL to create the source table accordingly</b><br></br>
# MAGIC <b>Please do the manual check of the records in the destination table by querying the records copied from source table and That are present in destination table</b>

# COMMAND ----------

from delta.tables import *
import os

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text('sourceTable','SAT_OSIPI_TAGDATA')
dbutils.widgets.text('destTable','SAT_OSIPI_TAGDATA_HIST_TST')
dbutils.widgets.text('logicalenv','dev01')
sourceTable = dbutils.widgets.get('sourceTable')
destTable = dbutils.widgets.get('destTable')
logicalenv = dbutils.widgets.get('logicalenv')
delta_folder_source = '/mnt/imrv/deltalake/im' + logicalenv + '/rawvault/' + sourceTable + '/'
delta_folder_dest = '/mnt/imrv/deltalake/im' + logicalenv + '/rawvault/' + destTable + '/'

# COMMAND ----------

print(sourceTable)
print(destTable)
print(logicalenv)

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace table im${logicalenv}_rawvault.${destTable}_bkp
# MAGIC DEEP CLONE im${logicalenv}_rawvault.${destTable}
# MAGIC LOCATION 'dbfs:/mnt/imrv/deltalake/im${logicalenv}/rawvault/${destTable}_bkp/'

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace table im${logicalenv}_rawvault.${sourceTable}_bkp
# MAGIC DEEP CLONE im${logicalenv}_rawvault.${sourceTable}
# MAGIC LOCATION 'dbfs:/mnt/imrv/deltalake/im${logicalenv}/rawvault/${sourceTable}_bkp/'

# COMMAND ----------

daily_count = spark.read.format('delta').load(delta_folder_dest).count()
print(daily_count)

# COMMAND ----------

hist_count = spark.read.format('delta').load(delta_folder_source).count()
print(hist_count)

# COMMAND ----------

# MAGIC %sql
# MAGIC Insert into im${logicalenv}_rawvault.${destTable} select * from im${logicalenv}_rawvault.${sourceTable}

# COMMAND ----------

total_count = spark.read.format('delta').load(delta_folder_dest).count()
print(total_count)

# COMMAND ----------

if total_count == (daily_count + hist_count):
  print("Merge successful")
else:
  print("Merge count is different, exiting")
  dbutils.notebook.exit("Merge count is different, exiting")


# COMMAND ----------

deltaTable = DeltaTable.forPath(spark,delta_folder_source)

# COMMAND ----------

deltaTable.delete()

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS im${logicalenv}_rawvault.${sourceTable};

# COMMAND ----------

# Cleaning the Historical table 
if os.path.exists('/dbfs/'):
  print("Removinvg the historical table folder structure from delta lake")
  dbutils.fs.rm(delta_folder_source + '_delta_log/_tmp_path_dir',True)
  dbutils.fs.rm(delta_folder_source + '_delta_log/',True)
  dbutils.fs.rm(delta_folder_source,True)
  #os.rmdir('/dbfs'+ zip_location)
else:
  print("Zip and extracted files from earlier transaction are not present")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS im${logicalenv}_rawvault.SAT_OSIPI_TAGDATA_HIST;
# MAGIC 
# MAGIC CREATE TABLE im${logicalenv}_rawvault.SAT_OSIPI_TAGDATA_HIST
# MAGIC (
# MAGIC 	 `HUB_DEVICE_HSH_KEY`   CHAR(40)
# MAGIC 	,`LOAD_DATE_TIME`   TIMESTAMP
# MAGIC     ,`EXTRACT_DATE_TIME`   TIMESTAMP
# MAGIC 	,`RECORD_SOURCE`   VARCHAR(200)
# MAGIC 	,`SAT_OSIPI_EAI_TAGDATA_HSH_DIFF`    CHAR(40)
# MAGIC     ,`WEBID`   STRING
# MAGIC     ,`NAME`   STRING
# MAGIC 	,`PATH`   STRING
# MAGIC 	,`OSIPI_TIMESTAMP`   TIMESTAMP
# MAGIC 	,`VALUE`   STRING
# MAGIC 	,`VALUE_NAME`   STRING
# MAGIC 	,`GOOD`   BOOLEAN
# MAGIC 	,`DEVICE_TYPE`   STRING
# MAGIC 	,`ETL_INSERT_DATETIME`   TIMESTAMP
# MAGIC 	,`EVENTHUB_DATETIME`   TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (EXTRACT_DATE_TIME)
# MAGIC LOCATION 'dbfs:/mnt/imrv/deltalake/im${logicalenv}/rawvault/SAT_OSIPI_TAGDATA_HIST/';
# MAGIC 
# MAGIC ALTER TABLE im${logicalenv}_rawvault.SAT_OSIPI_TAGDATA_HIST SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);
