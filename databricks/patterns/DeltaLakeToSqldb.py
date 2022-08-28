# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake to SQL Database
# MAGIC 
# MAGIC ***
# MAGIC 
# MAGIC This notebook will input sql file via ADF parameter and frame dataset based on the based on business logic by SQL and load into SQL DB using spark connector <br>
# MAGIC  **Truncate** or **Append** the Target Table based on the input parameter <br>
# MAGIC Dataframe from SQL should have matching columns and dataype with target table <br>
# MAGIC 
# MAGIC ***
# MAGIC 
# MAGIC ** Modification History **
# MAGIC 
# MAGIC |Date | Author | Details|
# MAGIC |-----|--------|--------|
# MAGIC |16/06/2021 | Malay Chand Nandi | Intital Version |
# MAGIC |22/07/2021 | Bharathi | Updated Spark Connector |
# MAGIC |21/10/2021 | Bharathi | Added Spatial User Defined Function |

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

#------------------------------------------------------
# Output text to blob file
#------------------------------------------------------
def write_to_blob_file(blob_path, file_name, file_content):

  dbutils.fs.put("/mnt/log/" + blob_path + file_name, file_content, overwrite = True)

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

# User Defined function for Spatial lat conversion 

def st_x(x,y):
  p = pyproj.Proj("+proj=lcc +lat_0=-37 +lon_0=145 +lat_1=-36 +lat_2=-38 +x_0=1000000 +y_0=1000000 +ellps=GRS80 +units=mm +no_defs")
  x,y = p(x, y, inverse=True)
  return(x)
spark.udf.register("st_x", st_x)

# COMMAND ----------

# User Defined function for Spatial long conversion 

def st_y(x,y):
  p = pyproj.Proj("+proj=lcc +lat_0=-37 +lon_0=145 +lat_1=-36 +lat_2=-38 +x_0=1000000 +y_0=1000000 +ellps=GRS80 +units=mm +no_defs")
  x,y = p(x, y, inverse=True)
  return(y)
spark.udf.register("st_y", st_y)

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
    
    log_path = "outbound/"
    
    # Create the file name and path
    audit_file_name = feed_details_uncompressed['JobName']  + "_" + str(pipeline_run_id) + "_" + time.strftime('UTC_%Y-%m-%dT%H%M%S') + "_run.json"
    
    
    audit_file_path = log_path + feed_details_uncompressed['JobName'] + '/' + str(pipeline_run_id) + '/'
    print (audit_file_path)
    # Convert results to json format
    results_json = json.dumps(audit_dict, indent=4, sort_keys=True) 
 
    # Output the log
    write_to_blob_file(audit_file_path, audit_file_name, results_json)

# COMMAND ----------

# Read config parameters passed from ADF
#--------------------------------------
dbutils.widgets.text("config_param", "")
dbutils.widgets.text("run_date", "")
dbutils.widgets.text("pipeline_run_id_param", "")

#read the parameters passed from ADF
feed_details_uncompressed = json.loads(dbutils.widgets.get("config_param"))[0]
pipeline_run_id = dbutils.widgets.get("pipeline_run_id_param")
run_date = dbutils.widgets.get("run_date")

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

  # Print SQL to screen
  print(query)
  
  #execute the sql query and return the dataframe
  results = sqlContext.sql(query)

  return results


# COMMAND ----------


#------------------------------------------------------------------------------------------------------------
# Below parameters retrieved from databrics secret scope linked to key vault for connecting to ADLS and SqlDB
#------------------------------------------------------------------------------------------------------------
if feed_details_uncompressed['LogicalEnv'] == 'tst002':
  blobStorageAccount = "anausim"+ feed_details_uncompressed['LogicalEnv'][:3] + "2stg01"
else:
  blobStorageAccount = "anausim"+ feed_details_uncompressed['LogicalEnv'][:3] + "stg01"

keyName = blobStorageAccount + '-primary'

blobStorageKey = dbutils.secrets.get(scope = "stgstore", key=keyName)
tempStorageContainer = "transform"

#set blob account 
spark.conf.set("fs.azure.account.key." + blobStorageAccount + ".dfs.core.windows.net", blobStorageKey)

#configurations
targetTable = feed_details_uncompressed['StagingTable']
DatabaseKey = feed_details_uncompressed['DatabaseKey']
WriteMode = feed_details_uncompressed['WriteMode']

sql_file_path = "/dbfs/mnt/transform/" 

#Create empty dictionary
sqlParams = {}

#add logical env to sqlParams dictionary
sqlParams["logicalenv"] = feed_details_uncompressed['LogicalEnv']

#add datetime from the batch run
sqlParams["run_date"] = dbutils.widgets.get("run_date")

#add filter hour if exits
if 'filter_hour' in feed_details_uncompressed:
  sqlParams["filter_hour"] = feed_details_uncompressed['filter_hour']

#if sql parameters present in the configuration, add them to dictionary
if 'SqlParams' in feed_details_uncompressed:
  sqlParams.update(json.loads(feed_details_uncompressed['SqlParams']))
try:  
  #Get full path of the sql script  
  file = sql_file_path + feed_details_uncompressed['SqlFilePath']

  #Get the resultset from the Raw-Vault query into a dataframe
  sourceDF = execute_sql_file(file,sqlParams)
  
  #writes dataframe into SqlDB. Overwrites the table with the dataframe content each time job runs.
  jdbcConnstring = dbutils.secrets.get(scope = "dfstore", key=DatabaseKey)
  
  sourceDF.write.mode(WriteMode)\
    .format('com.microsoft.sqlserver.jdbc.spark')\
    .option('url', jdbcConnstring) \
    .option("dbtable", targetTable) \
    .option("truncate", "True")\
    .option("batchsize", "400000") \
    .option('numPartitions','100') \
    .option("tableLock", "true") \
    .option("schemaCheckEnabled", "false") \
    .save()

  log_string = "Completed loading into sqlDB"

except Exception as e:
  # Audit file
  print_log("ERROR: %s " % str(e))
  write_to_spark_audit_file(str(e), pipeline_run_id, 0, time.time() - job_start_time)
  exit(1)






