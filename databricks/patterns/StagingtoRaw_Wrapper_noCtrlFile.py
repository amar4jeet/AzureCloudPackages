# Databricks notebook source
# MAGIC %md ## ADFWrapper.py
# MAGIC ### 
# MAGIC ### ------------------------------------------------------------------------------------------------------------------
# MAGIC ### This notebook is for executing the script to move data from Staging to raw. It calls stagingtoraw.py.
# MAGIC ### This notebook takes the input arguments from ADF pipeline and then it encodes the arguments and then pass that to stagingtoraw.py notebook.
# MAGIC ### ------------------------------------------------------------------------------------------------------------------
# MAGIC #### Date         : 03/06/2021
# MAGIC #### Author       : Sumit Verma
# MAGIC #### 
# MAGIC ##### Modification History : 
# MAGIC ##### 

# COMMAND ----------

import json
import zlib
from base64 import b64encode, b64decode
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from datetime import datetime
import numpy as np

# COMMAND ----------

feedval = dbutils.widgets.get("config_param")

#Convert the received parameter string from ADF pipeline to Dictionary object
feedval = eval(feedval)
feed_details_uncompressed = {}

# Creating dictionary to pass to StagingtoRaw notebook
for val in feedval:
    feed_details_uncompressed[val["ParameterCode"]] = val["ParameterValue"]

feed_details_uncompressed["Source"] = feedval[0]["Source"]
feed_details_uncompressed["TableName"] = feedval[0]["TableName"]

# job_instance_id is only for logging purpose as this is already present as part of staging to raw notebook
feed_details_uncompressed["job_instance_id"]= "999991"

#User defined pipeline run id is captured for logging migration or historical load
if "load_type" in feed_details_uncompressed and feed_details_uncompressed['load_type']=="Hist_Data":
  feedRunID = dbutils.widgets.get("RunID").strip()
  feed_details_uncompressed["RunID"]= feedRunID

# COMMAND ----------

# The function is used to write to log file about the batch operation 
# of files insertion fo a given table as success or failure. 
def WriteLogFile(SourceName,tableName, fileName, logType, dateTimeObj):
  
  logFileName=""
  logFileNameWithTimeStamp=""
  
  #Capturing the current year, month, day, Hour, minute & second
  year = dateTimeObj.year
  month = "%02d" % dateTimeObj.month
  day = "%02d" % dateTimeObj.day
  hour = "%02d" % dateTimeObj.hour
  minute = "%02d" % dateTimeObj.minute
  second = "%02d" % dateTimeObj.second
  timestamp = str(year) + str(month) + str(day) + str(hour) + str(minute) + str(second)
  
  filepathDir = '/mnt/log/files_to_archive/' + SourceName + '/' + tableName + '/'
  filepathDirWithTimeStamp = '/mnt/log/files_to_archive/' + SourceName + '/' + tableName + '/' + str(year) + '/' + str(month) +'/' + str(day) +'/'
  
  if logType == "success":
    logFileName = "s2r_success_files.log"
    logFileNameWithTimeStamp = "s2r_success_files" + timestamp + ".log"
  if logType == "error":
    logFileName = "s2r_error_files.log"
    logFileNameWithTimeStamp = "s2r_error_files" + timestamp + ".log"
    
  #Creation of success/failed file log files and the file with timestamp
  logFileNamePath = filepathDir + logFileName
  logFileNameWithTimeStampPath = filepathDirWithTimeStamp + logFileNameWithTimeStamp 
 
  print(logFileNamePath)
  print(logFileNameWithTimeStampPath)
  #Deleting the existing success/error log files
  dbutils.fs.rm(logFileNamePath)
  
  #Adding the success/failed files to Log file
  dbutils.fs.put(logFileNamePath, fileName)
  
  #Copying the success/failed file to success/failes file with timestamp
  dbutils.fs.cp(logFileNamePath, logFileNameWithTimeStampPath)

# COMMAND ----------

# Get all the files from the data object path

# Staging Directory Path for the Data object path
directory = '/mnt/staging' + feed_details_uncompressed["data_object_path"]

dateTimeObj = datetime.now()
successFiles=""
errorFiles=""
folderExist=True

try:
  files = dbutils.fs.ls(directory)
except:
  folderExist = False

if folderExist == True:
  #Number of iterations/loops to be performed on the list of files from the staging folder

  if "NUMBER_OF_ITER" in feed_details_uncompressed:
    NUMBER_OF_ITER = int(feed_details_uncompressed["NUMBER_OF_ITER"])
  else:
    NUMBER_OF_ITER = 1

  #declare the list variable where all the file path are stored
  files_path_list=[]

  # Iterating through all the files for data load to raw vault 
  #storing the file path in the list
  for file in files:
    files_path_list.append(file.path)

  ## list of files stored in the below list based on the number_of_iter
  split_file_path_list = np.array_split(files_path_list, NUMBER_OF_ITER)

  ##iterating the filepath based on the split (number of iterations ) and passing the list of files to the data_object_name

  for file_paths in split_file_path_list:
    file_path_list_subset=file_paths.tolist()
    try:
      #print(file_path_list_subset)
      if NUMBER_OF_ITER >1:
        feed_details_uncompressed["data_object_name"] = file_path_list_subset
      else:
        feed_details_uncompressed["data_object_name"]='dbfs:/mnt/staging' + feed_details_uncompressed["data_object_path"]+'/'

      feed_details_uncompressedencoded = b64encode(zlib.compress(json.dumps(feed_details_uncompressed).encode("utf-8"))).decode("ascii")
      dbutils.notebook.run("StagingtoRaw_noCtrlFile.py", 0 , {"notebook-params": feed_details_uncompressedencoded})

      ##capturing all the list of successfiles
      for file_path in file_path_list_subset:
        file_name= file_path.replace("dbfs:/mnt/staging" + feed_details_uncompressed['data_object_path'], "")
        successFiles = successFiles + file_name + "\n"

    except:
      ##capturing all the list of errorFiles
      for file_path in file_path_list_subset:
        file_name= file_path.replace("dbfs:/mnt/staging" + feed_details_uncompressed['data_object_path'], "")
        errorFiles = errorFiles + file_name + "\n"
      
    
# Writing the success file names to Success log
#if successFiles!="":
WriteLogFile(feed_details_uncompressed["Source"],feed_details_uncompressed["TableName"], successFiles, "success", dateTimeObj)
#   print_log("INFO: Successfully written files are: " + "\n" + successFiles)

# In case of failure in file load, create of log of the failed files and fail the operation.
#if errorFiles!="":
WriteLogFile(feed_details_uncompressed["Source"],feed_details_uncompressed["TableName"], errorFiles, "error", dateTimeObj)
#   print_log("INFO: Failes files are " + "\n" + errorFiles)
if errorFiles!="":
  raise Exception("At least one file data load failed")
