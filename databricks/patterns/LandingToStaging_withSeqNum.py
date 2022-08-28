# Databricks notebook source
import os
import pyspark.sql.functions as F
import datetime
import ast

sourceName=dbutils.widgets.get('sourceName').strip()
tableName=dbutils.widgets.get('tableName').strip()
fileList=dbutils.widgets.get('fileList').strip()
#print('sourceName:' ,sourceName)
#print('tableName:' ,tableName)
file_name_list=[]
#print('input fileName:' ,fileList)


# COMMAND ----------

#This function will archive file from landing to archive location.
# sample file_archive("dbfs:/mnt/stagingtemp/CIS/ADR2/", "ADR2.D2021137.T025235883" , "dbfs:/mnt/archive/landing/CIS/ADR2/")
def file_archive(landingPath, fileName, archivePath):

  sdate=datetime.datetime.strptime(fileName.split('.')[1].replace('D',''), '%Y%j').date()
  landingPath = landingPath + fileName
  archivePath = archivePath + sdate.strftime('%Y') + "/" + sdate.strftime('%m') + "/" + sdate.strftime('%d') + "/" + fileName
  
  return dbutils.fs.mv(landingPath,archivePath)

# COMMAND ----------

#This funcion will take the input parameter as temp file path, file name and staging path.
#it will read the csv file, add file path and write it to one parquet file in staging.
def temp_to_staging_parquet(temp_path,file_name,stg_path):
  data = spark.read.format('csv').load(temp_path.replace('/dbfs','dbfs:')+file_name,inferSchema=False, header=False)
  prop_col_list=['Prop_'+str(i) for i in range(0,len(data.columns))]
  data=data.toDF(*prop_col_list)
  data=data.withColumn("_adfFilePath",F.input_file_name())
  #display(data)
  data.coalesce(1).write.mode('overwrite').parquet(stg_path+file_name)
  for src_path in dbutils.fs.ls(stg_path+file_name):
    if 'part-' in src_path.path:
      tgt_file_path=stg_path+file_name+'.parquet'
      #print(tgt_file_path)
      dbutils.fs.rm(tgt_file_path)
      dbutils.fs.mv(src_path.path,tgt_file_path)
      #print(src_path.path)
      dbutils.fs.rm(stg_path+file_name, True)
  return data


# COMMAND ----------

#Function to add sequence number to each line of the file contents and write it to temp path.
def rec_seq_add(landing_path,file_name,temp_path):
  fout = open(temp_path+file_name, 'w')
  # write thie file into /mnt/ml/tmp/source/tablename/filename.csv
  with open(landing_path+file_name) as fin:
    for i, line in enumerate(fin):
      fout.write(line.strip('\n')+','+str(i+1)+'\n')
  fin.close()
  fout.close()


# COMMAND ----------

container='landing'
#container='stagingtemp'  #for testing purpose
temp_file_path= '/dbfs/mnt/ml/tmp/'+sourceName+'/'+tableName+'/'
dbutils.fs.mkdirs('dbfs:/mnt/ml/tmp/'+sourceName)
dbutils.fs.mkdirs(temp_file_path.replace('/dbfs','dbfs:'))

landing_path='/dbfs/mnt/'+container+'/'+sourceName+'/'+tableName+'/'
stg_path='/mnt/staging/'+sourceName+'/'+tableName+'/'
archivePath='dbfs:/mnt/archvault/landing/'+sourceName+'/'+tableName+'/'

log_path='/dbfs/mnt/log/L2S_seq_num/'+sourceName+'/'+tableName+'/'+datetime.datetime.now().date().strftime('%Y')+'/'+datetime.datetime.now().date().strftime('%m')+'/'+datetime.datetime.now().date().strftime('%d')
print('log file path:',log_path)
log_file_name='L2S_'+sourceName+'_'+tableName+'_'+datetime.datetime.now().strftime('%Y%m%d%H%M%S')+'.log'
dbutils.fs.mkdirs(log_path.replace('/dbfs','dbfs:'))
print('log file name:',log_file_name)

for file in ast.literal_eval(fileList):
  file_name_list.append(landing_path+file.get("name"))
#print('file_name list', file_name_list)
#print ('Number of files to be processed : ', len(file_name_list))
fout_log = open(log_path+'/'+log_file_name, 'w')
fout_log.write('file_name,staging_row_count,load_ts\n')
try:
  for fi in file_name_list:
    file_name=fi.split('/')[-1]
    rec_seq_add(landing_path,file_name,temp_file_path)
    df=temp_to_staging_parquet(temp_file_path,file_name,stg_path)
    rc=file_archive(landing_path.replace('/dbfs','dbfs:'), file_name, archivePath)
    if rc == True:
      #print("successfully archived")
      fout_log.write(file_name+','+str(df.count())+','+str(datetime.datetime.now())+'\n')
    else:
      #print('file archive failed.')
      fout_log.close()
      exit(1)
  fout_log.close()
except Exception as e:
  fout_log.close()
  print("Encountered error: %s " % str(e))
  exit(1)
