# Databricks notebook source
# MAGIC %md # Raw to Synapse
# MAGIC ## 
# MAGIC ## ------------------------------------------------------------------------------------------------------------------
# MAGIC ## This notebook execute a view or table in databricks and loads data into staging table in synapse
# MAGIC ## Configurations are passed from ADF. Keys are read from keyvault, which is configured to databricks secret scope
# MAGIC ## ------------------------------------------------------------------------------------------------------------------
# MAGIC ### Date         : 20/01/2020
# MAGIC ### Author       : NS  
# MAGIC ### 
# MAGIC #### Modification History: 
# MAGIC ####  

# COMMAND ----------

pip install pyproj

# COMMAND ----------

#------------------------------------------------------
# Imports
#------------------------------------------------------
import zlib
from base64 import b64encode, b64decode
import json
from delta.tables import *
import time
from pyspark.sql.functions import col, hash, sha1, lit, concat, concat_ws
from pyspark.sql.functions import trim, UserDefinedFunction
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
import numpy as np
from datetime import date
import datetime

import pyproj

# COMMAND ----------

#------------------------------------------------------
# Global variables
#------------------------------------------------------

global log_string
global job_start_time

log_string = ""
job_start_time = time.time()

# COMMAND ----------

##############################################################################
# The below functuion used to extract Latitide and Longitude column values
##############################################################################
def st_x(x,y):
  p = pyproj.Proj("+proj=lcc +lat_0=-37 +lon_0=145 +lat_1=-36 +lat_2=-38 +x_0=1000000 +y_0=1000000 +ellps=GRS80 +units=mm +no_defs")
  x,y = p(x, y, inverse=True)
  return(x)
spark.udf.register("st_x", st_x)

def st_y(x,y):
  p = pyproj.Proj("+proj=lcc +lat_0=-37 +lon_0=145 +lat_1=-36 +lat_2=-38 +x_0=1000000 +y_0=1000000 +ellps=GRS80 +units=mm +no_defs")
  x,y = p(x, y, inverse=True)
  return(y)
spark.udf.register("st_y", st_y)

# COMMAND ----------

#------------------------------------------------------
# Output text to screen and log
#------------------------------------------------------

def print_log(log_text):
    #Format of time so its human readable
    txt = time.strftime("%Y-%m-%d %H:%M:%S %Z", time.localtime()) + " - " + log_text
    #log_string = log_string + "\n" + txt
    print(txt)

# COMMAND ----------

#------------------------------------------------------
# Output text to blob file
#------------------------------------------------------
def write_to_blob_file(blob_path, file_name, file_content):

  dbutils.fs.put("/mnt/log/" + blob_path + file_name, file_content, overwrite = True)

# COMMAND ----------

#------------------------------------------------------
# Write to spark audit file
#------------------------------------------------------
def write_to_spark_audit_file(log, pipeline_run_id, job_exe_status, job_exe_runtime):
    
    # set the audit values
    audit_dict = {}
    audit_dict["pipeline_run_id"] = pipeline_run_id
    audit_dict["job_exe_status"] = job_exe_status
    audit_dict["job_exe_runtime"] = str(job_exe_runtime) + "s"
    audit_dict["log"] = log.split('\n')
    audit_dict["feed_details_uncompressed"] = feed_details_uncompressed
    
    log_path = "semanticLayer/"
    
    # Create the file name and path
    audit_file_name = feed_details_uncompressed['JobName']  + "_" + str(pipeline_run_id) + "_" + time.strftime('UTC_%Y-%m-%dT%H%M%S') + "_run.json"
    
    
    audit_file_path = log_path + feed_details_uncompressed['JobName'] + '/' + str(pipeline_run_id) + '/'
    print (audit_file_path)
    # Convert results to json format
    results_json = json.dumps(audit_dict, indent=4, sort_keys=True) 
 
    # Output the log
    write_to_blob_file(audit_file_path, audit_file_name, results_json)

# COMMAND ----------

dbutils.widgets.text("config_param", "")
dbutils.widgets.text("run_date", "")
dbutils.widgets.text("pipeline_run_id_param", "")
dbutils.widgets.text("synpase_jdbc_conn_string", "")

#read the parameters passed from ADF
feed_details_uncompressed = json.loads(dbutils.widgets.get("config_param"))[0]
pipeline_run_id = dbutils.widgets.get("pipeline_run_id_param")
run_date = dbutils.widgets.get("run_date")
synpase_jdbc_conn_string = dbutils.widgets.get("synpase_jdbc_conn_string")
print(str(feed_details_uncompressed))


# COMMAND ----------

#------------------------------------------------------------------------------
# This function reads a sql file and returns output of the query in a dataframe
# sql parameters are passed as dictionary 
#------------------------------------------------------------------------------

def execute_sql_file(sqlFile, sqlParams):

  with open(sqlFile) as fr:
    query = fr.read()
    
  #remove $ before the parameter name, for easy format with dictionary keys  
  query = query.replace('${', '{')  

  #replace the parameters present in the file with the dictionary  
  query = query.format(**sqlParams)

  #execute the sql query and return the dataframe
  results = sqlContext.sql(query)

  return results


# COMMAND ----------

#------------------------------------------------------
# Generate and add hash key to dataframe for given columns
#------------------------------------------------------
def get_df_hash_for_column_list(pk_col_list, col_list, df, hash_key_col, hash_diff_col):

  #list of all decimal columns used in hsh diff
  decimal_columns = []
  
  #transform nulls into '' to include them in hashing
  udf = UserDefinedFunction(lambda x: '' if x is None else x, StringType())
  df = df.select(*[udf(column).alias(column) if "_truncated" in column else column for column in df.columns])    
  
  #get all columns to get hash
  for idx, item in enumerate(col_list):
    col_list[idx] = item   
    
  hash_diff = sha1(concat(concat_ws(':',*col_list),lit(':')))
 
  #Get primary keys 
  if pk_col_list is not None:
    for idx, item in enumerate(pk_col_list):
      pk_col_list[idx] = item   
      
    hash_key = sha1(concat(concat_ws(':',*pk_col_list),lit(':')))
  else:
    hash_key = lit('')
    
 
 
  return df.withColumn(hash_key_col, hash_key ).withColumn(hash_diff_col, hash_diff )

# COMMAND ----------


#------------------------------------------------------
# Get hash diff for the columns in the dataframe
#------------------------------------------------------
def get_df_hash_diff(df, businessKey):
 
  #create empty dictionary to hold column names
  sat_values_map = {}
  print("business key: " + str(businessKey))
  if businessKey is not None and str(businessKey) != '':
    pk_columns = [col.strip() for col in businessKey.split(',')]
  else:
    pk_columns = None
  
  diff_columns = []
  hash_diff_col="HSH_DIFF"
  hash_key_col="HSH_KEY"
  
  #Build details required for table load
  #Mapping source to target columns
  for col in list(df.columns):
    sat_values_map["`" + col + "`"] = "s.`" + col + "`"
    
    if "HSH_" not in col\
     and "RUN_DTM" not in col\
     and "RECORD_INSERT_DTM" not in col\
     and "RECORD_UPDATE_DTM" not in col\
     and "TIMELINE_FROM_" not in col:
      diff_columns.append(col)
  
  
  #get hash columns for all the columns from the view
  df = get_df_hash_for_column_list(pk_columns, diff_columns, df, hash_key_col,hash_diff_col)
  
  return df

# COMMAND ----------


#------------------------------------------------------
# Below parameters retrieved from databrics secret scope linked to key vault
# Authentication with service principals is not supported for loading data into and unloading data from Azure Synapse.
#------------------------------------------------------
if feed_details_uncompressed['LogicalEnv'] == 'tst002':
  blobStorageAccount = "anausim"+ feed_details_uncompressed['LogicalEnv'][:3] + "2stg01"
else:
  blobStorageAccount = "anausim"+ feed_details_uncompressed['LogicalEnv'][:3] + "stg01"

keyName = blobStorageAccount + '-primary'
#jdbcKeyName = "imdw-jdbc-xlarge-connstring" 
if len(synpase_jdbc_conn_string) > 0:
          jdbcKeyName = synpase_jdbc_conn_string
else:
          jdbcKeyName = "imdw-jdbc-admin-connstring"


blobStorageKey = dbutils.secrets.get(scope = "stgstore", key=keyName)
tempStorageContainer = "transform"

#set blob account 
spark.conf.set("fs.azure.account.key." + blobStorageAccount + ".dfs.core.windows.net", blobStorageKey)

#prepare connection string for sqldw
sqldwConnstring = dbutils.secrets.get(scope = "dfstore", key=jdbcKeyName)

#configurations
stgTable = feed_details_uncompressed['StagingTable']
targetTable = feed_details_uncompressed['TargetTable']
businessKey = feed_details_uncompressed['BusinessKeys']

sql_file_path = "/dbfs/mnt/transform/" 

#Create empty dictionary
sqlParams = {}

#add logical env to sqlParams dictionary
sqlParams["logicalenv"] = feed_details_uncompressed['LogicalEnv']

#add datetime from the batch run
# For Migration data load, run_date passed as '1900-01-01'
if run_date == '1900-01-01':
  run_date = datetime.date.today() + datetime.timedelta(days=1)
  sqlParams["run_date"] = run_date
else:
  sqlParams["run_date"] = run_date

#if sql parameters present in the configuration, add them to dictionary
if 'SqlParams' in feed_details_uncompressed:
  sqlParams.update(json.loads(feed_details_uncompressed['SqlParams']))
try:  
  #Get full path of the sql script  
  file = sql_file_path + feed_details_uncompressed['SqlFilePath']

  #Get all records from the view into a dataframe
  sourceDF = execute_sql_file(file,sqlParams)

  #find hash key for all columns from the dataframe and append as a column
  if businessKey is not None and str(businessKey) != '':
    sourceDF = get_df_hash_diff(sourceDF, businessKey)
    stgTableOptions ="CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH(HSH_KEY)" 
  else:
    stgTableOptions ="CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN" 
    
  tempDir = "abfss://" + tempStorageContainer + "@" + blobStorageAccount + ".dfs.core.windows.net/temp/"
    
  #writes dataframe into synapse staging table. Overwrites the table.
  sourceDF.write \
    .format("com.databricks.spark.sqldw") \
    .option("url", sqldwConnstring) \
    .option("useAzureMSI" ,"true") \
    .option("dbTable", stgTable) \
    .option("tableOptions",stgTableOptions) \
    .option("maxStrLength","2000") \
    .option("tempDir", tempDir) \
    .mode("overwrite").save()
  
  log_string = "Completed loading into synapse"

except Exception as e:
  # Audit file
  print_log("ERROR: %s " % str(e))
  write_to_spark_audit_file(str(e), pipeline_run_id, 0, time.time() - job_start_time)
  exit(1)


# COMMAND ----------

#------------------------------------------------------
# Final output of log upon successful job
#------------------------------------------------------

#write_to_spark_audit_file(log_string,pipeline_run_id, 0, time.time() - job_start_time)

# COMMAND ----------


