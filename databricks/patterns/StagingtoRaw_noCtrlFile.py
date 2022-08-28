# Databricks notebook source
# MAGIC %md ## Staging To Raw
# MAGIC ### 
# MAGIC ### ------------------------------------------------------------------------------------------------------------------
# MAGIC ### This notebook moves data from the staging environment through to the raw vault as part of staging to rawvault data load pattern
# MAGIC ### All data object metadata is passed from wrapper Script.
# MAGIC ### Parameters are compressed and base64 encoded in transfer. 
# MAGIC ### ------------------------------------------------------------------------------------------------------------------
# MAGIC #### Date         : 07/07/2021
# MAGIC #### Author       : SV (Cloned and re-factor from ARMs StagintToRaw python notebook)

# COMMAND ----------

# MAGIC %md
# MAGIC Modification History:
# MAGIC 
# MAGIC NN | Date | Author | Comments |
# MAGIC ---|------|--------|----------|
# MAGIC 001| 11/01/2022 | Julia Vassileff | Performance improvement - added logic to filter on partition column (config parameter filter_on_partition_column). |
# MAGIC 002| 11/01/2022 | Julia Vassileff | Logic to exclude column ETL_INSERT_DATETIME from HashDiff calculation. |

# COMMAND ----------

#------------------------------------------------------
# Imports
#------------------------------------------------------
import zlib
from base64 import b64encode, b64decode
import json
import collections
import time
import datetime
from delta.tables import *
from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql.types import StringType,StructType,StructField,DateType,TimestampType,IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import *


# COMMAND ----------

#------------------------------------------------------
# Global variables
#------------------------------------------------------
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", 'LEGACY')
spark.conf.set("spark.sql.legacy.timeParserPolicy",'CORRECTED')
spark.conf.set("spark.sql.ansi.enabled",'false')
spark.conf.set("spark.sql.storeAssignmentPolicy",'LEGACY')
global log_string
global job_start_time

log_string = ""
job_start_time = time.time()
retry_attempts = 10
retry_sleep_duration = 120

# COMMAND ----------

#------------------------------------------------------
# Output text to blob file ???
#------------------------------------------------------
def write_to_blob_file(blob_path, file_name, file_content):
  dbutils.fs.put("/mnt/imlake/" + blob_path + file_name, file_content, overwrite = True)

# COMMAND ----------

#------------------------------------------------------
# Write to spark audit file ???
#------------------------------------------------------
def write_to_spark_audit_file(log_string, job_instance_id, job_exe_status, job_exe_runtime):
    
    # set the audit values
    audit_dict = {}
    audit_dict["job_instance_id"] = job_instance_id
    audit_dict["job_exe_status"] = job_exe_status
    audit_dict["job_exe_runtime"] = str(job_exe_runtime) + "s"
    audit_dict["log"] = log_string.split('\n')
    audit_dict["feed_details_uncompressed"] = feed_details_uncompressed
    
    # Create the file name and path
    audit_file_name = str(job_instance_id) + "_" + str(time.time()) + "_run.json"
    # ??? change the mount point name
    audit_file_path = 'cf/hdi-spark/rawvault/' + feed_details_uncompressed['SourceTablePrefix'] + '/' + str(job_instance_id) + '/'
    print (audit_file_path)
    # Convert results to json format
    results_json = json.dumps(audit_dict, indent=4, sort_keys=True) 
 
    # Output the log
    write_to_blob_file(audit_file_path, audit_file_name, results_json)

# COMMAND ----------

#------------------------------------------------------
# Write parquet file to blob for use by external table
#------------------------------------------------------  
def write_file_for_ext_table(db_name, tbl_name):
  print_log("Writing file for Polybase SQLDW")
  df= spark.sql('select * from ' + db_name + '.' + tbl_name)
  dbutils.fs.rm("/mnt/imhdi/hive/warehouse/polybase/im" + feed_details_uncompressed['logicalenv'] + '/' + tbl_name, True) 
  df.write.parquet("/mnt/imhdi/hive/warehouse/polybase/im" + feed_details_uncompressed['logicalenv'] + '/' + tbl_name)

# COMMAND ----------

#-------------------------------------------------------------------------------------------------------------------
# trans function will be called to transform the individual parameters based on the parameters declared
# This function will accept 4 parameters 
# df- dataframe : augmented dataframe which needs to be transformed 
# config -feed_details_uncompressed : to get all the config parameter values for the given parameter code
# trans_type- Transformation type passed from the ADF config
# col- column which needs to be transformed/ used in transformation
#-------------------------------------------------------------------------------------------------------------------


def trans(df, config, trans_type, col,custom_trans_name=None):
  
  if trans_type == "TRANSFORM_RAWTYPE":
    print("Converting RAW datatype to string")
    df = df.withColumn(col,F.hex(F.unbase64(F.col(col)).cast(StringType())))
  
  if trans_type == "TRANSFORM_TRIM_LEADING_ZERO":
    print("transfunctionTRANSFORM_TRIM_LEADING_ZERO")
    df = df.withColumn(col, F.trim(F.col(col))).withColumn(col, F.regexp_replace((col), r'^[0]*', '')).fillna({col:''})
   
  if trans_type == "TRANSFORM_TRIM_DECIMAL":
    print("transfunctionTRANSFORM_TRIM_DECIMAL")
    df = df.withColumn(col,F.regexp_replace(F.col(col),'\..*$',''))
    
  if trans_type == "TRANSFORM_PARTITION_COL":
    print("trans TRANSFORM_PARTITION_COL")
    df =  df.withColumn("temp_extract_date_partition", F.coalesce(F.to_date(F.col('EXTRACT_DATE_TIME'), "yyyy-MM-dd HH:mm:ss"),F.to_date(F.col('EXTRACT_DATE_TIME'), "yyyy-MM-dd HH:mm:ss.SSSSSS"),F.to_date(F.col('EXTRACT_DATE_TIME'), "yyyy-MM-dd"))).withColumn(col, F.to_timestamp("temp_extract_date_partition", "yyyy-MM-dd"))
    
  if trans_type == "TRANSFORM_DEFAULT_PARTITION":
    print("default partitions")
    df =  df.withColumn(col, F.to_timestamp(F.lit('')))
    
  if trans_type == "TRANSFORM_PII":
    print("trans TRANSFORM_PII")
    pii_value=json.loads(config[trans_type])[col]
    df= df.withColumn(col,F.lit(pii_value))
    
  if trans_type == "TRANSFORM_CHECKSUM":
    config_param_value=json.loads(config[trans_type])
    print(config_param_value)
    print("trans RANSFORM_CHECKSUM")
    df= df.withColumn(config_param_value[col]['targetcolumn'],F.col('{}'.format(col)).substr(config_param_value[col]['checksuminit'],config_param_value[col]['init'])).withColumn(col,F.col('{}'.format(col)).substr(config_param_value[col]['init'],config_param_value[col]['length']))
    
  if trans_type == "TRANSFORM_SUBSTRING":
    config_param_value=json.loads(config[trans_type])
    print(config_param_value)
    print("trans TRANSFORM_SUBSTRING")
    df= df.withColumn(config_param_value[col]['targetcolumn'],F.col('{}'.format(col)).substr(config_param_value[col]['init'],config_param_value[col]['length']))
    
  
  if trans_type == "TRANSFORM_REGEXP":
    config_param_value=json.loads(config[trans_type] if custom_trans_name is None else config[custom_trans_name])
    print(config_param_value)
    print("trans REGEXP")
    df= df.withColumn(config_param_value[col]['targetcolumn'],F.regexp_extract(F.col('{}'.format(col)),config_param_value[col]['pattern'],config_param_value[col]['matchGroup']))
    
  if trans_type == "TRANSFORM_REGEXP_EXTRACT_ALL":
    config_param_value=json.loads(config[trans_type] if custom_trans_name is None else config[custom_trans_name])
    print(config_param_value)
    print("trans REGEXP_EXTRACT_ALL")
    # There is no REGEXP_EXTRACT_ALL function available in dataframe api (Spark 3.1.1) so using spark sql for the same.
    df.createOrReplaceTempView("tempDf")
    if 'joinAll' in config_param_value[col] and config_param_value[col]['joinAll']=='Y':
      df=spark.sql("""select *,array_join(regexp_extract_all({0},'{1}',{2}),'') as {3} from tempDf """.format(col,config_param_value[col]['pattern'],config_param_value[col]['matchGroup'],config_param_value[col]['targetcolumn']))
    else:
      df=spark.sql("""select *,regexp_extract_all({0},'{1}',{2}) as {3} from tempDf """.format(col,config_param_value[col]['pattern'],config_param_value[col]['matchGroup'],config_param_value[col]['targetcolumn']))

  if trans_type == "TRANSFORM_PREFIX":
    config_param_value=json.loads(config[trans_type])
    print(config_param_value)
    print("trans TRANSFORM_PREFIX")
    df =df.withColumn(config_param_value[col]['targetcolumn'],df[col].substr(F.lit(config_param_value[col]['init']), F.lit(config_param_value[col]['length']))).withColumn(col,df[col].substr(F.lit(config_param_value[col]['prefix_init']), F.length(col)-F.lit(config_param_value[col]['pos'])))
   
  else:
    print('Skipping the transformation as it is not defined yet for the given column')
          
  return df

# COMMAND ----------

#------------------------------------------------------
# This function is used to extract HUB and LINK INFO parameters into new dictionaries 
# as part of merging multiple HUB/LINK_INFO paramaters into a single param
#------------------------------------------------------
def split_info(sourcedict, string):
    newdict = {}
    for key in sourcedict.keys():
        if key.startswith(string):
            newdict[key] = sourcedict[key]
    return newdict

# COMMAND ----------

# Update Record Source for migration

def getRecordSource(df):
    df=df.withColumn('RECORD_SOURCE',F.col('_srcFileName'))
    return df
   

# COMMAND ----------

#------------------------------------------------------
# Parameters
#------------------------------------------------------
dbutils.widgets.text("notebook-params", "")
feed_details = dbutils.widgets.get("notebook-params")
# #Accept parameters passed from wrapper script and uncompress to dictionary
feed_details_uncompressed = json.loads(zlib.decompress(b64decode(feed_details)))
print(feed_details_uncompressed)
#Extract Hub and Link Info key/values from feed_details_uncompressed into new separate dictionaries
hub_info_dict = split_info(feed_details_uncompressed, 'HUB_INFO')
link_info_dict = split_info(feed_details_uncompressed, 'LINK_INFO')

#Extract Prop info key values from feed_details_uncompressed into new separate dictionaries.
#These values are required for header mappings.
Prop_dict = split_info(feed_details_uncompressed, 'Prop')
Prop_col_dict={}

proplength = len(Prop_dict)

# The below iterative logic is to make sure that properties are arranged in descending order for column mappings
for index in range(proplength):
  Prop_col_dict["Prop_"+ str(index)] = Prop_dict["Prop_"+ str(index)]
  
#Remove Hub and Link Info key/values from the feed_details_uncompressed dict
for k in list(feed_details_uncompressed.keys()):
    if (k.startswith('HUB_INFO') or k.startswith('LINK_INFO')):
        del feed_details_uncompressed[k]
    
for k in list(feed_details_uncompressed.keys()):
    if (k.startswith('Prop')):
        del feed_details_uncompressed[k]
        
#Merge multiple Hub and Link Info dictionary key/values into a single HUB_INFO or LINK_INFO key
hub_info = ""
for k in list(hub_info_dict.keys()):
  hub_info += hub_info_dict[k]
link_info = ""
for k in list(link_info_dict.keys()):
  link_info += link_info_dict[k]

#Append hub and link dict back into feed_details_uncompressed
feed_details_uncompressed.update( {'HUB_INFO' : hub_info} )
feed_details_uncompressed.update( {'LINK_INFO' : link_info} )

# COMMAND ----------

#------------------------------------------------------
# Output text to screen and log
#------------------------------------------------------
def print_log(log_text):
    global log_string
    #Fix format of time so its human readable
    #txt = str(time.time()) + " - " + log_text
    txt = time.strftime("%Y-%m-%d %H:%M:%S %Z", time.localtime()) + " - " + log_text
    log_string = log_string + "\n" + txt

# COMMAND ----------

#------------------------------------------------------
# Add process metadata columns to dataframe migration
#------------------------------------------------------

def get_df_process_metadata_migration(df):
  
  return df.withColumn('RECORD_SOURCE',F.col('_srcFileName')).withColumn('LOAD_DATE_TIME', F.lit(datetime.datetime.now())).withColumn('CHANGE_TYPE',F.lit('I'))

# COMMAND ----------

#---------------------------------------------------------------------------------------------------------------------------
# For same business key and extract_date_time value, extract the latest record and eliminate the old records from dataframe
#---------------------------------------------------------------------------------------------------------------------------
def get_latest_transaction(df, partition_key_list):
  
  print(partition_key_list)
  
  w = Window.partitionBy(*partition_key_list).orderBy(desc(F.col('REC_SEQ').cast(IntegerType())))
  df = df.withColumn('Rank',row_number().over(w))
  df=df.filter(df.Rank == 1).drop(df.Rank)
  
  return df

# COMMAND ----------

#------------------------------------------------------
# Read Source field names into dataframes 
#------------------------------------------------------
# Using the extract files obtain field names from header record and rely upon datatypes being detected from actual data  
# Potential for replacement with explicit schemas depending on outcomes

#In case of no header mappings(Non-replication Sources)
if proplength == 0:    
  df_csv = spark.read.format('parquet').load(feed_details_uncompressed['data_object_name'])
  if "load_type" in feed_details_uncompressed and feed_details_uncompressed['load_type']=="Hist_Data":
    df_csv=df_csv.withColumn('RECORD_SOURCE',F.col('_srcFileName')).withColumn('LOAD_DATE_TIME', F.lit(datetime.datetime.now())).withColumn('EXTRACT_DATE_TIME',F.col('{0}'.format(feed_details_uncompressed['BUSINESSPARTITIONDATE']))).withColumn('CHANGE_TYPE',F.lit('I'))
  else:
    if 'TRANSFORM_PARSE_FILEDATE' in feed_details_uncompressed:
      decoder = json.JSONDecoder(object_pairs_hook=collections.OrderedDict)
      for col in decoder.decode(feed_details_uncompressed['TRANSFORM_PARSE_FILEDATE']):
        df_csv = trans(df_csv, feed_details_uncompressed, "TRANSFORM_REGEXP_EXTRACT_ALL" if 'matchAll' in json.loads(feed_details_uncompressed['TRANSFORM_PARSE_FILEDATE'])[col] and json.loads(feed_details_uncompressed['TRANSFORM_PARSE_FILEDATE'])[col]['matchAll']=="Y" else "TRANSFORM_REGEXP" ,col,"TRANSFORM_PARSE_FILEDATE")
    stringtotimestamp =  F.UserDefinedFunction(lambda x: datetime.datetime.strptime(x, '%Y%m%d%H%M%S'), TimestampType())
    df_csv=df_csv.withColumn('EXTRACT_DATE_TIME',stringtotimestamp(F.col('tempdate'))).withColumn('RECORD_SOURCE',F.lit(feed_details_uncompressed['RecordSource'])).withColumn('LOAD_DATE_TIME',F.lit(datetime.datetime.now())).withColumn('CHANGE_TYPE',F.lit('I'))

    
    
#     splitFilenames = feed_details_uncompressed['data_object_name'].split('_')
#     extractdate = splitFilenames[len(splitFilenames)-1].split('.')[0]
#     print(extractdate)
#     t=datetime.datetime.strptime(extractdate,'%Y%m%d%H%M%S')
#     d = [{'EXTRACT_DATE':t}]
#     df_ctl = spark.createDataFrame(d)
  
  
#In case of header mapping(Replication Sources)
newColumns = []
if proplength > 0:
  for k in list(Prop_col_dict.keys()):
    newColumns.append(Prop_col_dict[k])

  data = spark.read.format('parquet').load(feed_details_uncompressed['data_object_name'],                                          inferSchema=True, header=False)
  
  print(data.count())
  #Addition of CLIENT_ID column for HUB_EQUIPMENT data.
  for keyVal in feed_details_uncompressed:
    if feed_details_uncompressed[keyVal] == "HUB_EQUIPMENT":
      data = data.withColumn("CLIENT_ID", F.lit(100))
      break
      #client-id in hub_key
  
  oldColumns = data.schema.names
  
  print(oldColumns)
  print(newColumns)
  
  df = reduce(lambda data, idx: data.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), data)
  
  
  
  
  if "GENERATE_CDC_SEQNO" in feed_details_uncompressed and feed_details_uncompressed['GENERATE_CDC_SEQNO'] == "Y":
    if 'BUSINESS_KEY' in feed_details_uncompressed:
      partition_key_list = ["DM_TIMESTAMP","_adfFilePath"]
      for col in eval(feed_details_uncompressed['BUSINESS_KEY']):
        partition_key_list.append(col)
      
      print(feed_details_uncompressed['BUSINESS_KEY'])
      print(partition_key_list)
      
      df = get_latest_transaction(df, partition_key_list)
      
  ## source missing columns logic
  if "SourceMissingColumns" in feed_details_uncompressed:
    missingcols= feed_details_uncompressed["SourceMissingColumns"] 
    missingcolsList= missingcols.split(',')
    for val in missingcolsList:
      df = df.withColumn(val, F.lit(0))
      
  ##added the below logic for partitioning the data based on parameters in ADF config for migration  
  if "load_type" in feed_details_uncompressed and feed_details_uncompressed['load_type']=="Hist_Data":
    df_csv=get_df_process_metadata_migration(df)
  ## added the parameter Businesspartition date which is used to map Extract_date_time  
    if "BUSINESSPARTITIONDATE" in feed_details_uncompressed:
      if dict(df_csv.dtypes)[feed_details_uncompressed['BUSINESSPARTITIONDATE']]=='string':
          ##conversion of string to time stamp value which is used to populate extract date time
          
#           stringtodate =  F.UserDefinedFunction(lambda x: datetime.datetime.strptime(x, '%Y%m%d'), DateType())
          partition_col=feed_details_uncompressed['BUSINESSPARTITIONDATE']
#           df_csv=df_csv.withColumn("temp_extract_date_varchar_partition",F.date_format(stringtodate(F.col(partition_col).cast(StringType())),       'yyyy-MM-dd')).withColumn('EXTRACT_DATE_TIME',F.to_timestamp("temp_extract_date_varchar_partition", "yyyy-MM-dd"))
          df_csv=df_csv.withColumn('EXTRACT_DATE_TIME',F.coalesce(F.to_timestamp(F.col(partition_col).cast(StringType()), "yyyyMMdd"),F.to_timestamp(F.col(partition_col).cast(StringType()), "yyyyMMddHHmmss"),F.to_timestamp(F.col(partition_col).cast(StringType()), "yyyyMMddHHmmss.SSSSSSS"),F.col('DM_TIMESTAMP')))
          
          ##time stamp value which is used to populate extract date time
      elif dict(df_csv.dtypes)[feed_details_uncompressed['BUSINESSPARTITIONDATE']]=='timestamp':
          df_csv=df_csv.withColumn('EXTRACT_DATE_TIME',F.col('{}'.format(feed_details_uncompressed['BUSINESSPARTITIONDATE'])))
          #raise exception if it doesnt matches the above scenarios
      else:
          raise Exception("Please use valid timestamp field in business date partition config ")
    else:
      df_csv=df_csv.withColumn('EXTRACT_DATE_TIME',F.col('DM_TIMESTAMP'))
      
  else:
    #Applied the Change type filter to eliminate the duplicates as this record is said to be before update
    df=df.filter(df.CHANGE_TYPE != "B")
  
    df_csv = df.select([c for c in df.columns if c not in {'DM_TXID','DM_USER','_adfFilePath'}])
  #made this change for Extract_date_time mapped exactly same with source record.
    df_csv=df_csv.withColumn('RECORD_SOURCE',F.lit(feed_details_uncompressed['RecordSource'])).withColumn('LOAD_DATE_TIME', F.lit(datetime.datetime.now())).withColumn('EXTRACT_DATE_TIME',F.col('DM_TIMESTAMP'))
  
#   t=datetime.datetime.strptime(df_csv.select(F.col("DM_TIMESTAMP")).first()[0],'%Y-%m-%d %H:%M:%S')
#   d = [{'EXTRACT_DATE':t}]
#   df_ctl = spark.createDataFrame(d)

# COMMAND ----------

#------------------------------------------------------
# Add process metadata columns to dataframe
#------------------------------------------------------
def get_df_process_metadata(df, record_source, df_ctl):
  extract_date_time = df_ctl.select("EXTRACT_DATE").first()[0]
  
  return df.withColumn('RECORD_SOURCE',F.lit(record_source)).withColumn('LOAD_DATE_TIME', F.lit(datetime.datetime.now())).withColumn('EXTRACT_DATE_TIME',F.lit(extract_date_time))


# COMMAND ----------

#-------------------------------------------------------
# Generate and add hash key to dataframe            ----
#-------------------------------------------------------
def get_df_hash_for_column_list(col_list, df, key_name, def_key = '', def_key_val = '', same_as_link = 'N'):
#   print_log(str(col_list))

  print(col_list)
  for item in col_list:
    df = df.withColumn(item + "_truncated",F.trim(F.col(item)))

  udf = F.UserDefinedFunction(lambda x: '' if x is None else x, StringType())
  df = df.select(*[udf(column).alias(column) if "_truncated" in column else column for column in df.columns])      

  if same_as_link == 'Y' and len(def_key.strip()) > 0:
    return df.withColumn('concatenated_keys',F.concat(F.concat_ws(':',*col_list),F.lit(':'))).withColumn(key_name, F.sha1(F.col('concatenated_keys'))).withColumn(def_key, F.lit(def_key_val)) 
  else:
    for idx, item in enumerate(col_list):
      col_list[idx] = item + "_truncated" 
    return df.withColumn('concatenated_keys',F.concat(F.concat_ws(':',*col_list),F.lit(':'))).withColumn(key_name, F.sha1(F.col('concatenated_keys')))

# COMMAND ----------

# Map Business Keys for Hub
#------------------------------------------------------
def get_df_business_keys(df, hub_name, hub_info):
  hub_key_list = []
  #??? add config parameter
  # Updated HUB Tables business key column population logic to handle loading of multiple source columns values to same 
  # business key  column of HUB table in different rows(same-as-link scenario)

  if 'SALINK_HUB_NAME' in feed_details_uncompressed and feed_details_uncompressed['SALINK_HUB_NAME'] == hub_name:
    
    hub_key_list = [] 
    print(hub_info)
    # File has same-as-link Hub and the current Hub is having same-as-link mapping
    if(json.loads(feed_details_uncompressed[hub_info])['key_mapping']):
      for item in json.loads(feed_details_uncompressed[hub_info])['key_mapping']:
        key = json.loads(feed_details_uncompressed[hub_info])['key_mapping'][item]
        
        if json.loads(feed_details_uncompressed[hub_info])['key_mapping'][item] in hub_key_list:
          df = df.union(df.withColumn(json.loads(feed_details_uncompressed[hub_info])['key_mapping'][item], F.col(item)).withColumn('RECORD_SOURCE', F.lit(feed_details_uncompressed['RecordSource'] + "." + item)))     
        else:
          hub_key_list.append(json.loads(feed_details_uncompressed[hub_info])['key_mapping'][item])
          df = df.withColumn(json.loads(feed_details_uncompressed[hub_info])['key_mapping'][item], F.col(item)).withColumn('RECORD_SOURCE', F.lit(feed_details_uncompressed['RecordSource'] + "." + item))
          
  else:
    # The File does not have any Hub having same-as-link mapping
    if(json.loads(feed_details_uncompressed[hub_info])['key_mapping']):
      for item in json.loads(feed_details_uncompressed[hub_info])['key_mapping']:
        df = df.withColumn(json.loads(feed_details_uncompressed[hub_info])['key_mapping'][item], F.col(item)).withColumn('RECORD_SOURCE', F.lit(feed_details_uncompressed['RecordSource']))

  
  return df

# COMMAND ----------

#------------------------------------------------------
# Generate and add hash key to dataframe - version with additional decimal processing
#------------------------------------------------------
def get_df_hash_for_column_list_sat(col_list, df, key_name, dict_ddl):
  
  print_log(str(col_list))
  
  #list of all decimal columns used in hsh diff
  decimal_columns = []
  
  #skipping trim for decimals to avoid datatype change
  for item in col_list:    
    
    if 'decimal' in str(dict_ddl[item]) or 'double' in str(dict_ddl[item]):
      df = df.withColumn(item + "_truncated",F.col(item))
      decimal_columns.append(item + "_truncated")
    else:
      df = df.withColumn(item + "_truncated",F.trim(F.col(item)))
      
  #remove insignificant 0 from decimal values to match migration
  udf1 = F.UserDefinedFunction(lambda x: str(x).rstrip('0').rstrip('.') if '.' in str(x) else str(x), StringType())
  df = df.select(*[udf1(column).alias(column) if  "_truncated" in column and column in decimal_columns else column for (column, colType) in df.dtypes])
  
  #transform nulls into '' to include them in hashing
  udf = F.UserDefinedFunction(lambda x: '' if x is None else x, StringType())
  df = df.select(*[udf(column).alias(column) if "_truncated" in column else column for column in df.columns])    
    
  for idx, item in enumerate(col_list):
    col_list[idx] = item + "_truncated"  
  
  return df.withColumn('concatenated_keys',F.concat(F.concat_ws(':',*col_list),F.lit(':'))).withColumn(key_name, F.sha1(F.col('concatenated_keys')))

# COMMAND ----------

#------------------------------------------------------
# Maintain single hub
# Note, dataframe must be augmented with:
#   - business key mapped columns
#   - hash keys
#   - process metadata columns
#------------------------------------------------------
def maintain_hub(hub_name, df):
  
  start_time = time.time() 

  if "DB_MNT_LOCATION" in feed_details_uncompressed and hub_name in json.loads(feed_details_uncompressed['DB_MNT_LOCATION']).keys():
    db_mnt_location = json.loads(feed_details_uncompressed['DB_MNT_LOCATION'])[hub_name].strip()
    deltaTable = DeltaTable.forPath(spark, db_mnt_location + feed_details_uncompressed['logicalenv'] + "/rawvault/" + hub_name)    
  else:
    deltaTable = DeltaTable.forPath(spark, "/mnt/imrv/deltalake/im" + feed_details_uncompressed['logicalenv'] + "/rawvault/" + hub_name)
  
  values_map = {}
  #Retrieve columns and datatypes directly from table
  #Potential future refactor to use DataObjectField CF tables
  hub_ddl = spark.sql("describe im" + feed_details_uncompressed['logicalenv'] + "_rawvault." + hub_name)
  #Drop excess rows and columns
  hub_ddl = hub_ddl.filter("data_type != ''") 
  hub_ddl = hub_ddl.filter("data_type != 'RECORD_SOURCE'") 
  hub_ddl = hub_ddl.drop("comment")

  #Build details required for table load
  #Mapping source to target columns
  for col in hub_ddl.toPandas().to_dict(orient='list')['col_name']:
    values_map[col] = "s." + col
  
    if "HSH_KEY" in col:
      join_string = "s." + col + " = " + "t." + col
  #Perform a deltalake merge operation to load the data to the table
  #in-place operation and append to table if record does not exist
  deltaTable.alias("t").merge(
    df.alias("s"),
    join_string) \
  .whenNotMatchedInsert(
    values = values_map
  ).execute()
  
  print_log("INFO: Maintaining HUB; %s is complete and took %s seconds." % (hub_name , (str(time.time() - start_time))))

  if "RefreshExtractTable" in feed_details_uncompressed and feed_details_uncompressed['RefreshExtractTable'] == "Y":
    print_log("INFO: Writing files for SQLDW external table")
    write_file_for_ext_table("im" + feed_details_uncompressed['logicalenv'] + "_rawvault", hub_name)    
  
  return 0

# COMMAND ----------

#------------------------------------------------------
# Replace escape chars 
# This is done due to legacy of Hive limitations
#------------------------------------------------------

#Cound number of fields and put field names into list
dflength = len(df_csv.columns) 
collist = df_csv.columns

stringcols = []
#Loop through each field and determine its datatype. If string append to new list
for i in range(0, dflength):
    datatype = df_csv.dtypes[i]
    if "string" in datatype and "CHANGETYPE" not in datatype and "MANDT" not in datatype and "CLIENT" not in datatype:
      stringcols.append("`" + collist[i] + "`")

#For string columns only replace hive escape chars and set none to ''
udf = F.UserDefinedFunction(lambda x: x if x is None else x.replace('\¿','¿').replace('\¡','¡').replace('\,',',').replace('\"','"'), StringType())
df = df_csv.select(*[udf(column).alias(column) if "`" + column + "`" in stringcols else column for column in df_csv.columns])

#----calling Transformation functions below to transform the source columns------------------------#

if 'TRANSFORM_RAWTYPE' in feed_details_uncompressed:
  for col in eval(feed_details_uncompressed['TRANSFORM_RAWTYPE']):
    df = trans(df, feed_details_uncompressed, "TRANSFORM_RAWTYPE", col)

if 'TRANSFORM_PREFIX' in feed_details_uncompressed:
  for col in eval(feed_details_uncompressed['TRANSFORM_PREFIX']):
    df = trans(df, feed_details_uncompressed, "TRANSFORM_PREFIX", col)

if 'TRANSFORM_TRIM_LEADING_ZERO' in feed_details_uncompressed:
  for col in eval(feed_details_uncompressed['TRANSFORM_TRIM_LEADING_ZERO']):
    df = trans(df, feed_details_uncompressed, "TRANSFORM_TRIM_LEADING_ZERO", col)
    
if 'TRANSFORM_CHECKSUM' in feed_details_uncompressed:
  for col in eval(feed_details_uncompressed['TRANSFORM_CHECKSUM']):
    print(col)
    df = trans(df, feed_details_uncompressed, "TRANSFORM_CHECKSUM", col)
        
if 'TRANSFORM_SUBSTRING' in feed_details_uncompressed:
  for col in eval(feed_details_uncompressed['TRANSFORM_SUBSTRING']):
    df = trans(df, feed_details_uncompressed, "TRANSFORM_SUBSTRING", col)

if 'TRANSFORM_REGEXP' in feed_details_uncompressed:
  for col in eval(feed_details_uncompressed['TRANSFORM_REGEXP']):
    df = trans(df, feed_details_uncompressed, "TRANSFORM_REGEXP", col)
    
if 'TRANSFORM_TRIM_DECIMAL' in feed_details_uncompressed:
  for col in eval(feed_details_uncompressed['TRANSFORM_TRIM_DECIMAL']):
    df = trans(df, feed_details_uncompressed, "TRANSFORM_TRIM_DECIMAL", col)
    
if 'TRANSFORM_REGEXP_EXTRACT_ALL' in feed_details_uncompressed:
  for col in eval(feed_details_uncompressed['TRANSFORM_REGEXP_EXTRACT_ALL']):
    df = trans(df, feed_details_uncompressed, "TRANSFORM_REGEXP_EXTRACT_ALL", col)
    
    

# COMMAND ----------

##Below function returns the subset dataframe based on filters applied in config to load the respective hubs and satellites
#eg below
#SPLIT_HUB = {"KLART" : {"HUB_EQUIPMENT" : "IS2" , "HUB_MATERIAL": "003", "HUB_NMI" : "001" }}
##passing the augmented data frame,tablename,split_hub parameter to get the subset of data for each hub/satellite
def get_subset_df(df,splittype,tablename):
  
  split_col_name, split_table_dict = next(iter(splittype.items()))
  split_col_val = split_table_dict[tablename]
  df=df.filter(df[split_col_name] == split_col_val)
  
  return df

# COMMAND ----------

/#------------------------------------------------------
# Iterate through and maintain Hubs 
#------------------------------------------------------
# Prepare the dataframe for hub processing
# SALink Commented below line and added function call inside for loop for each hub_info
# df_augmented = get_df_business_keys(df_augmented)

#This change for Extract_date_time mapped exactly same with source record.
#for non replication source we are calling the metadata function where proplength=0 and for replication sources we have changed the logic to fetch the data from file.

#commented below part as we have same scenarios for all the load types

# if proplength>0 or (proplength == 0 and "load_type" in feed_details_uncompressed and feed_details_uncompressed['load_type']=="Hist_Data") or proplength == 0:
       #df_augmented = df
# else:
#   #df_augmented = df
#   df_augmented = get_df_process_metadata(df, feed_details_uncompressed['RecordSource'], df_ctl)

df_augmented = df
  
for i in  range(0, int(feed_details_uncompressed['HubCount'])):
    hub_name = feed_details_uncompressed["Hub_" + str(i+1) + "_Name"]
    
    hub_info = str("HUB_" + str(i+1) + "_INFO")
    df_augmented = get_df_business_keys(df_augmented, hub_name, hub_info)
    
#     if "TRANSFORM_HUB_KEY" in feed_details_uncompressed:
#       trans_hub_key_list = list(json.loads(feed_details_uncompressed["TRANSFORM_HUB_KEY"]).keys())
#       hub_business_key_list = json.loads(feed_details_uncompressed["HUB_" + str(i+1) + "_INFO"])[hub_name]
#       for item in hub_business_key_list:
#         if item in trans_hub_key_list:          
#           df_augmented = trans(df_augmented, feed_details_uncompressed,'TRANSFORM_HUB_KEY', item)
    

#     if 'TRANSFORM_TRIM_LEADING_ZERO' in feed_details_uncompressed:
#       for col in eval(feed_details_uncompressed['TRANSFORM_TRIM_LEADING_ZERO']):
#         df_augmented = trans(df_augmented, feed_details_uncompressed, "TRANSFORM_TRIM_LEADING_ZERO", col)
     
    df_augmented = get_df_hash_for_column_list(json.loads(feed_details_uncompressed["HUB_" + str(i+1) + "_INFO"])[hub_name], df_augmented, hub_name + "_HSH_KEY")
    
    ### added this code to handle the split logic to load the multiple tables from single source file ####
   
    if "SPLIT_HUB" in feed_details_uncompressed and hub_name in str(json.loads(feed_details_uncompressed["SPLIT_HUB"]).values()):
      print(hub_name,"split")
      df_augmented_subset=get_subset_df(df_augmented,json.loads(feed_details_uncompressed["SPLIT_HUB"]),hub_name)
      
    else:
      df_augmented_subset=df_augmented
      
    #Reduce dataframe to single record per hash key
    df_augmented_hub = df_augmented_subset.dropDuplicates([hub_name + "_HSH_KEY"])
    #df_augmented.show()
    
    #Update Record Source For Migration
    #if "load_type" in feed_details_uncompressed and feed_details_uncompressed['load_type']=="Hist_Data": --commented as part of MSATS changes
    #Added as part of MSATS
    if ("load_type" in feed_details_uncompressed and feed_details_uncompressed['load_type']=="Hist_Data") or ("Source" in feed_details_uncompressed and feed_details_uncompressed['Source']=="MSATS"):
      df_augmented_hub=getRecordSource(df_augmented_hub)

    #Maintain the hub
    hub_retry_attempts = retry_attempts
    for i in range(hub_retry_attempts):
      print_log("INFO: Maintaining HUB; %s..." % hub_name)
      try:
        print("testing record_source column population")
        maintain_hub(hub_name, df_augmented_hub) 
      except Exception as e:
        print_log("ERROR: Load attempt: %s - Encountered error: %s " % (str(i + 1),str(e)) )
        #add append to separate blob for next run to see number of occurances
        #Check for concurrency error and max retries not met
        if "oncurrent" in str(e) and i < hub_retry_attempts - 1:
          print_log("INFO: Concurrent write error detected.  Retrying to load %s - try number: %s" % (hub_name, str(i + 2)) )
          time.sleep(retry_sleep_duration)
          continue
        #Check for failure due to AWB non-standard HSH column names
        elif "in search condition given columns" in str(e) and i < hub_retry_attempts - 1:
          #This code to be removed once the AWB technical debt is addressed
          print_log("INFO: AWB HUB table detected. Re-jigging parameters to suit. Retrying to load %s - try number: %s" % (hub_name, str(i + 2)) )
          df_augmented_hub = df_augmented_hub.withColumnRenamed(hub_name + "_HSH_KEY",hub_name.replace("AWB_", "" ) + "_ID_HSH_KEY")
          df_augmented = df_augmented.withColumnRenamed(hub_name + "_HSH_KEY",hub_name.replace("AWB_", "" ) + "_ID_HSH_KEY")
          continue
        else:
          write_to_spark_audit_file(log_string, feed_details_uncompressed['job_instance_id'], 0, time.time() - job_start_time)
          exit(1)
      break

df_augmented = df_augmented.withColumn('RECORD_SOURCE', F.lit(feed_details_uncompressed['RecordSource']))

#Update record source for Migration Data.
#if "load_type" in feed_details_uncompressed and feed_details_uncompressed['load_type']=="Hist_Data": --commented as part of MSATS
#Added as part of MSATS
if ("load_type" in feed_details_uncompressed and feed_details_uncompressed['load_type']=="Hist_Data") or ("Source" in feed_details_uncompressed and feed_details_uncompressed['Source']=="MSATS"):
  df_augmented=getRecordSource(df_augmented)


# COMMAND ----------

#------------------------------------------------------
# Maintain single link
# Note, dataframe must be augmented with:
#   - business key mapped columns
#   - hash keys including link hash keys
#   - process metadata columns
# Same logic is applied to HUBS
#------------------------------------------------------
def maintain_link(link_name, df):

  start_time = time.time()
  print_log("INFO: Maintaining Link; %s..." % link_name)
   
  deltaTable = DeltaTable.forPath(spark, "/mnt/imrv/deltalake/im" + feed_details_uncompressed['logicalenv'] + "/rawvault/" + link_name)
  
  values_map = {}

  print(link_name)
  lnk_ddl = spark.sql("describe im" + feed_details_uncompressed['logicalenv'] + "_rawvault." + link_name)
  
#   hub_ddl = spark.sql("describe im" + feed_details_uncompressed['logicalenv'] + "_rawvault." + hub_name)
  
  #Drop excess rows and columns
  lnk_ddl = lnk_ddl.filter("data_type != ''") 
  lnk_ddl = lnk_ddl.filter("data_type != 'RECORD_SOURCE'") 
  lnk_ddl = lnk_ddl.drop("comment")
 
  #Build details required for table load
  #Mapping source to target columns
  for col in lnk_ddl.toPandas().to_dict(orient='list')['col_name']:
    values_map[col] = "s." + col
  
    if "HSH_KEY" in col and "LNK_" in col:
      join_string = "s." + col + " = " + "t." + col
  #Perform a deltalake merge operation to load the data to the table
  #in-place operation and append to table if record does not exist
  deltaTable.alias("t").merge(
    df.alias("s"),
    join_string) \
  .whenNotMatchedInsert(
    values = values_map
  ).execute()

  print_log("INFO: Maintaining Link; %s is complete and took %s seconds." % (link_name , (str(time.time() - start_time))))
    
  if "RefreshExtractTable" in feed_details_uncompressed and feed_details_uncompressed['RefreshExtractTable'] == "Y":
    print_log("INFO: Writing files for SQLDW external table")
    write_file_for_ext_table("im" + feed_details_uncompressed['logicalenv'] + "_rawvault", link_name)    
    
  return 0

# COMMAND ----------

#------------------------------------------------------
# Iterate through and maintain Links 
#------------------------------------------------------

for i in  range(0, int(feed_details_uncompressed['LinkCount'])):

    link_name = feed_details_uncompressed["Link_" + str(i+1) + "_Name"]
  
    df_augmented = get_df_hash_for_column_list(json.loads(feed_details_uncompressed['LINK_INFO'])[link_name], df_augmented, link_name + "_HSH_KEY")

    #Reduce dataframe to single record per hash key
    df_augmented_link = df_augmented.dropDuplicates([link_name + "_HSH_KEY"])
    
    # Maintain the Links  
    link_retry_attempts = retry_attempts
    for i in range(link_retry_attempts):
      print_log("INFO: Maintaining LINK; %s..." % link_name)
      try:
        maintain_link(link_name, df_augmented_link) 
      except Exception as e:
        print_log("ERROR: Load attempt: %s - Encountered error: %s " % (str(i + 1),str(e)) )
        #Check for concurrency error and max retries not met
        if "oncurrent" in str(e) and i < link_retry_attempts - 1:
          print_log("INFO: Concurrent write error detected.  Retrying to load %s - retry number: %s" % (link_name, str(i + 2)) )
          time.sleep(retry_sleep_duration)
          continue
        else:
          write_to_spark_audit_file(log_string, feed_details_uncompressed['job_instance_id'], 0, time.time() - job_start_time)
          exit(1)
      break

# COMMAND ----------

#-----------------------------------------------------------------------------------------------------------------------------------------------------
# Maintain same_as_link:
# - If same-as-link is set to 'Y' and same-as-link table is given then extract all key parameters specific to same-as-link from salink_key_mapping
# - Create the same-as-link key definition columns from the corresponding source columns
# - Create and add hash keys(same-as-link table HSH columns) for the corresponding key definition columns to the augmented DF 
# - Create and add hash keys for same-as-link table to the augmented DF
# - Call maintain_link function to insert data to same-as-link table
#-----------------------------------------------------------------------------------------------------------------------------------------------------

same_as_link = ""
salink_name = ""
load_salink = False 

if "SAME_AS_LINK" in feed_details_uncompressed and "SA_LINK_NAME" in feed_details_uncompressed:
  same_as_link = feed_details_uncompressed["SAME_AS_LINK"].strip()
  salink_name = feed_details_uncompressed["SA_LINK_NAME"].strip()

if same_as_link == 'Y' and len(salink_name) > 0:
  for key in json.loads(feed_details_uncompressed['SALINK_INFO'])['salink_key_mapping']:
    src_col_list = json.loads(feed_details_uncompressed['SALINK_INFO'])['salink_key_mapping'][key]
    key_seq = key.split('_')[-1]
    
    for item in json.loads(feed_details_uncompressed['SALINK_KEY_DEF_INFO'])['salink_key_def_mapping']:
      if key_seq + "_DEF" in item:
        def_key = item
        def_key_val = json.loads(feed_details_uncompressed['SALINK_KEY_DEF_INFO'])['salink_key_def_mapping'][item]
        df_augmented = get_df_hash_for_column_list(src_col_list, df_augmented, key, def_key, def_key_val, same_as_link)
        load_salink = True  
      
  # Create Primary key/Hash Key of same-as-link table
  if load_salink:
    df_augmented = get_df_hash_for_column_list(json.loads(feed_details_uncompressed['SALINK_INFO'])[salink_name], df_augmented, salink_name + "_HSH_KEY")
    #df_augmented = df_augmented.dropDuplicates([salink_name + "_HSH_KEY"]) 
    #commented this to take the data into seperate dataframe for same as link
    df_augmented_salink = df_augmented.dropDuplicates([salink_name + "_HSH_KEY"])
          
  link_retry_attempts = retry_attempts
  #if load_salink
  for i in range(link_retry_attempts):
    print_log("INFO: Maintaining LINK; %s..." % salink_name)
    try:
      maintain_link(salink_name, df_augmented_salink) 
      load_sat = True
    except Exception as e:
      print_log("ERROR: Load attempt: %s - Encountered error: %s " % (str(i + 1),str(e)) )
    #Check for concurrency error and max retries not met
      if "oncurrent" in str(e) and i < link_retry_attempts - 1:
        print_log("INFO: Concurrent write error detected.  Retrying to load %s - retry number: %s" % (link_name, str(i + 2)) )
        time.sleep(retry_sleep_duration)
        continue
      else:
        write_to_spark_audit_file(log_string, feed_details_uncompressed['job_instance_id'], 0, time.time() - job_start_time)
        exit(1)
    break

# COMMAND ----------

#----maintain logging for satellite tables 
#----capturing the record count file wise 

def maintain_log_sat(sat_name):
  schema= StructType([
    StructField('FileName', StringType(), True),
    StructField('Count', StringType(), True),
    ])
  ## log path #####
  log_path='/mnt/log/data_validation/HIST_LOAD' + '/' + feed_details_uncompressed["Source"] +'/' + feed_details_uncompressed["TableName"] +'/'+feed_details_uncompressed["RunID"] +'/S2R/'
  
  ## getting record count
  Sat_record_count = spark.sql("""select RECORD_SOURCE,cast (count(0) as string) myrowcnt from im""" + feed_details_uncompressed['logicalenv'] + '_rawvault.' + sat_name + """ GROUP BY RECORD_SOURCE """)
 
  ## before output logging
  dbutils.fs.mkdirs(log_path)
  Sat_record_count_out = spark.read.format('csv').schema(schema).load(log_path)
  ###Combining the output with current record count
  result=Sat_record_count.unionAll(Sat_record_count_out)
  ###capturing results
  result.coalesce(1).write.format('csv').mode('overwrite').save(log_path)

# COMMAND ----------

#------------------------------------------------------
# Maintain single sat
# Note, dataframe must be augmented with:
#   - hash keys
#   - hash diff keys
#   - process metadata columns
#------------------------------------------------------
def maintain_sat(sat_name, df):

  start_time = time.time()
  print_log("INFO: Maintaining Satelite; %s..." % sat_name)
  
  deltaTable = DeltaTable.forPath(spark, "/mnt/imrv/deltalake/im" + feed_details_uncompressed['logicalenv'] + "/rawvault/" + sat_name)
  sat_values_map = {}

  sat_ddl = spark.sql("describe im" + feed_details_uncompressed['logicalenv'] + "_rawvault." + sat_name)
  #Code Change : Remove partition columns(part 0, part 1 etc) from satellite table ddl
  if "TRANSFORM_PARTITION_COL" in feed_details_uncompressed:
    for item in eval(feed_details_uncompressed['TRANSFORM_PARTITION_COL']):
      sat_ddl = sat_ddl.filter("data_type != '{0}'".format(item))
      
  if "TRANSFORM_DEFAULT_PARTITION" in feed_details_uncompressed:
    for item in eval(feed_details_uncompressed['TRANSFORM_DEFAULT_PARTITION']):
      sat_ddl = sat_ddl.filter("data_type != '{0}'".format(item))
  
  #Drop excess rows and columns
  sat_ddl = sat_ddl.filter("data_type != ''") 
  sat_ddl = sat_ddl.filter("data_type != 'EXTRACT_DATE_TIME'") 
  
  if proplength > 0:
    # (TODO) MANI to remove this part once the source target mapping are clear
    sat_ddl = sat_ddl.filter("""col_name not like 'SRC_DEVICE_ID'""")
    sat_ddl = sat_ddl.filter("""col_name not like 'SRC_SERVICE_POINT_ID'""")

  sat_ddl = sat_ddl.drop("comment")
  
  diff_columns = []
  #Build details required for table load
  #Mapping source to target columns
  for col in sat_ddl.toPandas().to_dict(orient='list')['col_name']:
    sat_values_map["`" + col + "`"] = "s.`" + col + "`"
    if "HSH_KEY" in col:
      hash_key_col = col
    if "HSH_DIFF" in col:
      hash_diff_col = col
    if "HSH_" not in col \
      and "ETL_INSERT_DATETIME" not in col \
      and "LOAD_DATE_TIME" not in col \
      and "RECORD_SOURCE" not in col \
      and "EXTRACT_DATE_PARTITION" not in col:
      diff_columns.append(col)
      #and "EXTRACT_DATE_TIME" not in col \ 
      #Added EXTRACT_DATE_TIME in hash_diff colukn to insert latest row for same key.
  
  #get the dictionary for real datatypes in dataframe cl[0] - name, cl[1] - type
  dict_ddl = {}

  for cl in sat_ddl.collect():
    dict_ddl[cl[0]] = cl[1]
     
  df = get_df_hash_for_column_list_sat(diff_columns, df, hash_diff_col, dict_ddl)
  
  #added this to eliminate the duplicates based on the hash key and hash difference when we started getting the records with different change types
  df=df.dropDuplicates([hash_key_col,hash_diff_col])
  
  #Adding this to handle satellites that doesn't have partition keys and so as to treat satellite without CV.
  if ("DIRECTINSERT" in feed_details_uncompressed and feed_details_uncompressed["DIRECTINSERT"].strip()=='Y') or ("load_type" in feed_details_uncompressed and feed_details_uncompressed['load_type']=="Hist_Data"):
    df.select([i['col_name'] for i in sat_ddl.selectExpr("col_name").collect()]).createOrReplaceTempView('IncData')
    spark.sql(""" insert into im{0}_rawvault.{1} select * from IncData""".format(feed_details_uncompressed['logicalenv'],sat_name))

  else: 
    #Retrieve current HSH Key and HSH Diff field from target sat
    targetDfCV = spark.sql("SELECT distinct " + hash_key_col + ", " + hash_diff_col + " FROM im" + feed_details_uncompressed['logicalenv'] + "_rawvault." + sat_name + "_CV")
    #001 - Performance improvement
    if ("filter_on_partition_column" in feed_details_uncompressed.keys()) :
      where_clause = "1==0"
      partition_column_config = json.loads(feed_details_uncompressed["filter_on_partition_column"])
      for partition_column in spark.sql(f"DESCRIBE TABLE im{feed_details_uncompressed['logicalenv']}_rawvault.{sat_name}").filter("col_name like 'Part%'").collect():
        partition_col=partition_column["data_type"] 
        configuration_found = 0
        for config  in partition_column_config["partition_config"]:
          partition_config_col_name = config["partition_column_name"]
          if partition_config_col_name.lower() == partition_col.lower():
            partition_config_column_data_type = config["partition_column_data_type"]
            partition_config_column_date_format = config["partition_column_date_format"] 
            configuration_found = 1
            for partition_col_value in df.select(f"{partition_col}").distinct().collect():
              print(partition_col_value[partition_col])
              where_clause = where_clause + f" OR {partition_col} == to_timestamp('{str(partition_col_value[partition_col])}','{partition_config_column_date_format}')"
                           
        if configuration_found == 1 :
          print(f"SELECT {hash_key_col},{hash_diff_col}  FROM im{feed_details_uncompressed['logicalenv']}_rawvault.{sat_name} WHERE {where_clause}")
          targetDfCV = spark.sql(f"SELECT {hash_key_col},{hash_diff_col}  FROM im{feed_details_uncompressed['logicalenv']}_rawvault.{sat_name} WHERE {where_clause}")

    #Delta check
    #"left anti join" source with target. It takes all rows from the left dataset that don't have a match in the right 
    #df1 will then contain only new records
    df1 = df.join(targetDfCV, [hash_key_col, hash_diff_col], "leftanti")

    #Perform a deltalake merge operation to load the data to the table
    #in-place operation and append to table if record does not exist
    deltaTable.alias("t").merge(
      df1.alias("s"),
      '1 <> 1') \
    .whenNotMatchedInsert(
      values = sat_values_map
    ).execute()
  
  print_log("INFO: Maintaining Satelite; %s is complete and took %s seconds." % (sat_name , (str(time.time() - start_time))))
  
  if "RefreshExtractTable" in feed_details_uncompressed and feed_details_uncompressed['RefreshExtractTable'] == "Y":
    print_log("INFO: Writing files for SQLDW external table")
    write_file_for_ext_table("im" + feed_details_uncompressed['logicalenv'] + "_rawvault", sat_name)
  
  return 0

# COMMAND ----------

#-------------------------------------------------------------------------
# Apply tarnsformation on individual column based on the config parameter
# Iterate through and maintain Satellites
#-------------------------------------------------------------------------

if "TRANSFORM_PII" in feed_details_uncompressed:  
  trans_col_list = json.loads(feed_details_uncompressed['TRANSFORM_PII']).keys() 
  for item in trans_col_list:
    df_augmented = trans(df_augmented, feed_details_uncompressed, "TRANSFORM_PII", item)
    
    
if 'TRANSFORM_HOURLY_PARTITION' in feed_details_uncompressed:
  df_augmented = df_augmented.withColumn("Extract_hour", hour(F.col("EXTRACT_DATE_TIME")))\
  .withColumn("EXTRACT_DATE_PARTITION", F.to_timestamp(F.concat(F.coalesce(F.to_date("EXTRACT_DATE_TIME", "yyyy-MM-dd HH:mm:ss"),F.to_date("EXTRACT_DATE_TIME", "yyyy-MM-dd HH:mm:ss.SSSSSS"),F.to_date("EXTRACT_DATE_TIME", "yyyy-MM-dd")), F.concat(F.lit(" ") , F.col('Extract_hour'), F.lit(":00:00")))))
  
if 'TRANSFORM_DEFAULT_PARTITION' in feed_details_uncompressed:
  for col in eval(feed_details_uncompressed['TRANSFORM_DEFAULT_PARTITION']):
    df_augmented = trans(df_augmented, feed_details_uncompressed, "TRANSFORM_DEFAULT_PARTITION", col)
  
elif 'TRANSFORM_PARTITION_COL' in feed_details_uncompressed:
  for col in eval(feed_details_uncompressed['TRANSFORM_PARTITION_COL']):
    df_augmented = trans(df_augmented, feed_details_uncompressed, "TRANSFORM_PARTITION_COL", col)
else:
  print("No Partition column defined for this table")


for i in  range(0, int(feed_details_uncompressed['SatCount'])):

    sat_name = feed_details_uncompressed["Sat_" + str(i+1) + "_Name"]
    
     ### added this code to handle the split logic to load the multiple tables from single source file #### 
    
    if ("SPLIT_SAT" in feed_details_uncompressed and sat_name in str(json.loads(feed_details_uncompressed["SPLIT_SAT"]).values())):
      df_augmented_sat_subset = get_subset_df(df_augmented,json.loads(feed_details_uncompressed["SPLIT_SAT"]),sat_name)
      df_augmented_sat = df_augmented_sat_subset
      
    else:
      df_augmented_sat = df_augmented
      
    # Maintain the sat
    try:
      maintain_sat(sat_name, df_augmented_sat)
    except Exception as e:
      # Audit file
      print_log("ERROR: %s " % str(e))
      write_to_spark_audit_file(log_string, feed_details_uncompressed['job_instance_id'], 0, time.time() - job_start_time)
      exit(1)

# COMMAND ----------

if "load_type" in feed_details_uncompressed and feed_details_uncompressed['load_type']=="Hist_Data":
  for i in  range(0, int(feed_details_uncompressed['SatCount'])):
    sat_name = feed_details_uncompressed["Sat_" + str(i+1) + "_Name"]
    maintain_log_sat (sat_name)

# COMMAND ----------

#------------------------------------------------------
# Maintain single reference object
# Note, dataframe must be augmented with:
#   - process metadata columns
#------------------------------------------------------
def maintain_ref(ref_obj_name, df):
  
  start_time = time.time()
  deltaTable = DeltaTable.forPath(spark, "/mnt/imrv/deltalake/im" + feed_details_uncompressed['logicalenv'] + "/rawvault/" + ref_obj_name)
  
  values_map = {}
  #Retrieve columns and datatypes directly from table
  #Potential future refactor to use DataObjectField CF tables
  
  ref_ddl = spark.sql("describe im" + feed_details_uncompressed['logicalenv'] + "_rawvault." + ref_obj_name)
  #Drop excess rows and columns
  ref_ddl = ref_ddl.filter("data_type != ''") 
  ref_ddl.show()
  
  #Build details required for table load
  #Mapping source to target columns
  for col in ref_ddl.toPandas().to_dict(orient='list')['col_name']:
    
    values_map[col] = "s." + col
    join_string = "s." + col + " = " + "t." + col
      
  #These are the strings used to build the merge statement
  print("values_map is : ", values_map)
  print("join string is : ", join_string)

  # Truncate the delta table and load the data as-is since its a reference object
  spark.sql("truncate table im" + feed_details_uncompressed['logicalenv'] + "_rawvault." + ref_obj_name)
  
  #Perform a deltalake merge operation to load the data to the table
  #in-place operation and append to table if record does not exist
  deltaTable.alias("t").merge(
    df.alias("s"),
    join_string) \
  .whenNotMatchedInsert(
    values = values_map
  ).execute()

  
  print_log("INFO: Maintaining REFERENCE; %s is complete and took %s seconds." % (ref_obj_name , (str(time.time() - start_time))))

  if "RefreshExtractTable" in feed_details_uncompressed and feed_details_uncompressed['RefreshExtractTable'] == "Y":
    print_log("INFO: Writing files for SQLDW external table")
    write_file_for_ext_table("im" + feed_details_uncompressed['logicalenv'] + "_rawvault", ref_obj_name)    
  
  return 0

# COMMAND ----------

#------------------------------------------------------
# Iterate through and maintain Refs 
#------------------------------------------------------

#Prepare the dataframe for ref processing - augment with business keys to the dataframe
# df_augmented_ref = get_df_process_metadata(df, feed_details_uncompressed['RecordSource'], df_ctl)

#This change for Extract_date_time mapped exactly same with source record.
#for non replication source we are calling the metadata function where proplength=0 and for replication sources we have changed the logic to fetch the data from file.
# added load_type check to bypass migration jobs with out going to process metadata
#commented this code as we are not using the get_df_process_metadata for all other types

# if proplength == 0 and "load_type" not in feed_details_uncompressed:
#   df_augmented_ref = get_df_process_metadata(df, feed_details_uncompressed['RecordSource'], df_ctl)
# else:
# df_augmented_ref.take(2)

# Only call maintain_ref if REFERENCE_TABLE is defined in the encoding string
if('REFERENCE_TABLE' in feed_details_uncompressed):
  df_augmented_ref = get_df_process_metadata(df, feed_details_uncompressed['RecordSource'], df_ctl)
  
  if(feed_details_uncompressed['REFERENCE_TABLE'] == 'Y'):
      ref_name = feed_details_uncompressed["Ref_Name"]
      #Maintain the ref
      
      # Retry attempts for reference object is set to 1 since reference object is loaded infrequently and 
      # multiple reference objects will not be loaded at once
      ref_retry_attempts = 1
      for i in range(ref_retry_attempts):
        print_log("INFO: Maintaining REF; %s..." % ref_name)
        try:
          maintain_ref(ref_name, df_augmented_ref) 
        except Exception as e:
          # Audit file
          print_log("ERROR: %s " % str(e))
          write_to_spark_audit_file(log_string, feed_details_uncompressed['job_instance_id'], 0, time.time() - job_start_time)
          exit(1)
else:
    print_log("INFO: REFERENCE_TABLE key is not defined in encoding string, passing through this function")