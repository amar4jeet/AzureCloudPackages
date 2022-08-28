# Databricks notebook source
# MAGIC %md ## RVDirectInsert.py
# MAGIC ###  
# MAGIC ### ------------------------------------------------------------------------------------------------------------------
# MAGIC ### This notebook is for executing the script to move data from Staging to raw.
# MAGIC ### This notebook takes the input arguments from ADF pipeline and then pass that to the notebook.
# MAGIC ### ------------------------------------------------------------------------------------------------------------------
# MAGIC #### Date         : 03/30/2021
# MAGIC #### Author       : Amarjeet Kumar
# MAGIC ####  
# MAGIC ##### Modification History : 
# MAGIC #####

# COMMAND ------------

from pyspark.sql.functions import *
from datetime import datetime
from pyspark.sql.types import StringType,StructType,StructField

# COMMAND ----------

spark.conf.set("spark.sql.ansi.enabled",'false')
spark.conf.set("spark.sql.storeAssignmentPolicy",'LEGACY')
spark.conf.set("spark.databricks.delta.formatCheck.enabled",'false')
spark.conf.set("spark.sql.legacy.timeParserPolicy",'LEGACY')

# COMMAND ----------

#Parameterizing the variables to be used in the script
Staging_Location=dbutils.widgets.get('Staging_Location').strip()
Target_Location=dbutils.widgets.get('Target_Location').strip()
dm_rv_schema=dbutils.widgets.get('dm_rv_schema').strip()
#dbutils.widgets.text('SourceName','PN')
SourceName=dbutils.widgets.get('SourceName').strip()
Env=dbutils.widgets.get('Env').strip()
rv_table_name=dbutils.widgets.get('rv_table_name').strip()
tableName=dbutils.widgets.get('tableName').strip()
# dbutils.widgets.text('RunID','0')
RunID=dbutils.widgets.get('RunID').strip()

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
directory = Staging_Location 
filepathDir = '/mnt/log/files_to_archive/' + SourceName + '/' + tableName + '/'
files = dbutils.fs.ls(directory)
# print(files)
dateTimeObj = datetime.now()

successFiles=""
errorFiles=""  
#df = spark.read.load(path=Staging_Location)
values_map = {}
#Retrieve columns and datatypes directly from table
table_ddl = spark.sql("describe " + dm_rv_schema + "." + rv_table_name)

# removing partitioning columns from column headers
Partition_checkid=0
for i in table_ddl.selectExpr("col_name").collect():
  if (i['col_name'])=="# Partitioning":
    Partition_checkid=1
  if (Partition_checkid==1):
    print (i['col_name'])
    table_ddl = table_ddl.filter("col_name != '{0}'".format(i['col_name']))

#Drop excess rows and columns
table_ddl = table_ddl.filter("data_type != ''") 
table_ddl = table_ddl.drop("comment")
table_ddl.show()

#declare the list variable where all the file path are stored
files_path_list=[]
# Iterating through all the files for data load to raw vault 
#storing the file path in the list
for file in files:
  files_path_list.append(file.path)
try:
  df = spark.read.format('parquet').load(Staging_Location)
  if tableName=='SAT_UIQ_EVENT_LOG':
    df = df.withColumn("ROW_INSERT_DTM", to_date(col('ROW_INSERT_DTM'), "yyyy-MM-dd"))
  df.select([i['col_name'] for i in table_ddl.selectExpr("col_name").collect()]).createOrReplaceTempView('IncData')
  spark.sql("""insert into im{0}_hist_data.{1} select * from IncData""".format(Env,rv_table_name))
  
  for file_path in files_path_list:
      file_name= file_path.replace(Staging_Location, "")
      successFiles = successFiles + file_name + "\n"
except Exception as e:
  
  for file_path in files_path_list:
      file_name= file_path.replace(Staging_Location, "")
      errorFiles = errorFiles + file_name + "\n"
      print(e)
      
# Writing the success file names to Success log
if successFiles!="":
  WriteLogFile(SourceName, tableName, successFiles, "success", dateTimeObj)
  #print("INFO: Successfully written files are: " + "\n" + successFiles)

# In case of failure in file load, create of log of the failed files and fail the operation.
if errorFiles!="":
  WriteLogFile(SourceName,tableName, errorFiles, "error", dateTimeObj)
#   print_log("INFO: Failes files are " + "\n" + errorFiles)
  raise Exception("At least one file data load failed")

# COMMAND ----------

#----maintain logging for satellite tables 
#----capturing the record count file wise 

def maintain_log_sat():
  schema= StructType([
    StructField('FileName', StringType(), True),
    StructField('Count', StringType(), True),
    ])
  ## log path #####
  log_path='/mnt/log/data_validation/HIST_LOAD' + '/' + SourceName +'/' + tableName +'/'+ RunID +'/S2R/'
  
  ## getting record count
  Sat_record_count = spark.sql("""select '"""+ rv_table_name + """' TableName , count(0) myrowcnt from """ + dm_rv_schema + '.' + rv_table_name )
 
  ## before output logging
  dbutils.fs.mkdirs(log_path)
  Sat_record_count_out = spark.read.format('csv').schema(schema).load(log_path)
  ###Combining the output with current record count
  result=Sat_record_count.unionAll(Sat_record_count_out)
  ###capturing results
  result.coalesce(1).write.format('csv').mode('overwrite').save(log_path)

# COMMAND ----------

# Generating Satellite record count
maintain_log_sat ()
