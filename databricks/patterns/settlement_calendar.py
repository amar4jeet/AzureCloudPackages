# Databricks notebook source
#------------------------------------------------------
# Imports
#------------------------------------------------------
from pyspark.sql.functions import col
from pyspark.sql import SQLContext

# COMMAND ----------

#------------------------------------------------------
# Configuration
#------------------------------------------------------
spark.conf.set("parquet.enable.summary-metadata",'false')
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs",'false')
spark.conf.set("spark.sql.sources.commitProtocolClass",'org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol')

# COMMAND ----------

#Parameterizing the variables to be used in the script
# dbutils.widgets.text('inputfilePath','dbfs:/mnt/stagingtemp/Calendar/AEMO DD Calendar 2020.csv/')
inputfilePath=dbutils.widgets.get('inputfilePath').strip()
# dbutils.widgets.text('parquetfilepath','dbfs:/mnt/stagingtemp/Calendar/')
parquetfilepath=dbutils.widgets.get('parquetfilepath').strip()
# dbutils.widgets.text('parquetfile','REF_METER_DATA_CDR')
# parquetfile=dbutils.widgets.get('parquetfile').strip()
dbutils.widgets.text('deltalake_schema','imtst002_transform')
deltalake_schema=dbutils.widgets.get('deltalake_schema').strip()
# dbutils.widgets.remove('parquetfile')

# COMMAND ----------

#-----------------------------------------------------------------
# Reading csv File from blob container and creating dataframes
#-----------------------------------------------------------------
df=spark.read.format('csv').option('Header',True).load(inputfilePath)
df.select(col('Year'),col('Week Id'),col('Start'),col('End'),col('Prelim When'),col('Prelim Code'),col('Prelim Description'), col('Prelim Start'), col('Prelim End'),col('Revision 1 When'),col('Revision 1 Code'),col('Revision 1 Description'),col('Revision 1 Start'), col('Revision 1 End'),col('Final When'), col('Final Code'),col('Final Description'),col('Final Start'),col('Final End'), col('Revision 2 When'),col('Revision 2 Code'),col('Revision 2 Description'), col('Revision 2 Start'),col('Revision 2 End')).createOrReplaceTempView('INC_CDR')
df_hol=df.select(col('Year'),col('Holiday When'),col('Holiday Code'),col('Holiday Description')).dropna(subset=['Holiday When','Holiday Code']).filter(col('Holiday Code') !='n/a').filter(col('Holiday When') !='n/a').createOrReplaceTempView('HOL_CDR')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ** ------------------------------------------------------
# MAGIC -- ** Writing the data to target REF_AEMO_CDR 
# MAGIC -- Transform Schema REF_AEMO_CDR
# MAGIC -- ** ------------------------------------------------------
# MAGIC MERGE into $deltalake_schema.REF_AEMO_CDR as Target
# MAGIC USING INC_CDR
# MAGIC on Year=YR AND `Week Id`= WK
# MAGIC when matched then
# MAGIC UPDATE SET Target.BILL_PD_START_DT=Start,
# MAGIC Target.BILL_PD_END_DT=END
# MAGIC ,Target.ROW_INSERT_DTM=current_timestamp() 
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT(YR,WK,BILL_PD_START_DT,BILL_PD_END_DT,ROW_INSERT_DTM) 
# MAGIC values(INC_CDR.Year,INC_CDR.`Week Id`,
# MAGIC INC_CDR.Start,
# MAGIC INC_CDR.END,
# MAGIC current_timestamp())

# COMMAND ----------

#-----------------------------------------------------------------
# Generating parquet files- REF_AEMO_CDR
#-----------------------------------------------------------------

df = sqlContext.sql("SELECT * FROM " + deltalake_schema + ".ref_aemo_cdr")
# display(df)
output_path=parquetfilepath + "ref_aemo_cdr"
df.write.format("parquet").mode('overwrite').save(output_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- ** ------------------------------------------------------
# MAGIC -- ** Writing the data to target REF_METER_DATA_CDR
# MAGIC -- Transform Schema REF_METER_DATA_CDR --Preliminary Statement
# MAGIC -- ** ------------------------------------------------------
# MAGIC MERGE INTO $deltalake_schema.REF_METER_DATA_CDR as Target
# MAGIC USING INC_CDR
# MAGIC ON `Prelim Code`= TYP 
# MAGIC AND `Prelim When`= REQ_BY
# MAGIC 
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET FRM_DT= `Prelim Start`,
# MAGIC TO_DT = `Prelim End`,
# MAGIC DLVRY_DESC = `Prelim Description`,
# MAGIC ROW_INSERT_DTM=current_timestamp()
# MAGIC 
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT(REQ_BY,TYP,DLVRY_DESC,FRM_DT,TO_DT,SRC_YR,ROW_INSERT_DTM) 
# MAGIC VALUES(`Prelim When`,`Prelim Code`,`Prelim Description`,`Prelim Start`,`Prelim End`,`Year`,current_timestamp())

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- ** ------------------------------------------------------
# MAGIC -- ** Writing the data to target REF_METER_DATA_CDR
# MAGIC -- Transform Schema REF_METER_DATA_CDR -- Revision 1 (20-Week) Statement
# MAGIC -- ** ------------------------------------------------------
# MAGIC MERGE INTO $deltalake_schema.REF_METER_DATA_CDR as Target
# MAGIC USING INC_CDR
# MAGIC ON `Revision 1 Code`= TYP 
# MAGIC AND `Revision 1 When`= REQ_BY
# MAGIC 
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET FRM_DT= `Revision 1 Start`,
# MAGIC TO_DT = `Revision 1 End`,
# MAGIC DLVRY_DESC = `Revision 1 Description`,
# MAGIC ROW_INSERT_DTM=current_timestamp()
# MAGIC 
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT(REQ_BY,TYP,DLVRY_DESC,FRM_DT,TO_DT,SRC_YR,ROW_INSERT_DTM) 
# MAGIC VALUES(`Revision 1 When`,`Revision 1 Code`,`Revision 1 Description`,`Revision 1 Start`,`Revision 1 End`,`Year`,current_timestamp())

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ** ------------------------------------------------------
# MAGIC -- ** Writing the data to target REF_METER_DATA_CDR
# MAGIC -- Transform Schema REF_METER_DATA_CDR --Final Statement
# MAGIC -- ** ------------------------------------------------------
# MAGIC MERGE INTO $deltalake_schema.REF_METER_DATA_CDR as Target
# MAGIC USING INC_CDR
# MAGIC ON `Final Code` = TYP
# MAGIC AND `Final When` = REQ_BY
# MAGIC 
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET FRM_DT = `Final Start`,
# MAGIC TO_DT = `Final End`,
# MAGIC DLVRY_DESC = `Final Description`,
# MAGIC ROW_INSERT_DTM=current_timestamp()
# MAGIC 
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT(REQ_BY,TYP,DLVRY_DESC,FRM_DT,TO_DT,SRC_YR,ROW_INSERT_DTM)
# MAGIC VALUES( `Final When`,`Final Code`,`Final Description`,`Final Start`,`Final End`,`Year`,current_timestamp())

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- ** ------------------------------------------------------
# MAGIC -- ** Writing the data to target REF_METER_DATA_CDR
# MAGIC -- Transform Schema REF_METER_DATA_CDR --Revision 2 (30-Week) Statement
# MAGIC -- ** ------------------------------------------------------
# MAGIC MERGE INTO $deltalake_schema.REF_METER_DATA_CDR as Target
# MAGIC USING INC_CDR
# MAGIC ON `Revision 2 Code`= TYP 
# MAGIC AND `Revision 2 When`= REQ_BY
# MAGIC 
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET FRM_DT= `Revision 2 Start`,
# MAGIC TO_DT = `Revision 2 End`,
# MAGIC DLVRY_DESC = `Revision 2 Description`,
# MAGIC ROW_INSERT_DTM=current_timestamp()
# MAGIC 
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT(REQ_BY,TYP,DLVRY_DESC,FRM_DT,TO_DT,SRC_YR,ROW_INSERT_DTM) 
# MAGIC VALUES(`Revision 2 When`,`Revision 2 Code`,`Revision 2 Description`,`Revision 2 Start`,`Revision 2 End`,`Year`,current_timestamp())

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- ** ------------------------------------------------------
# MAGIC -- ** Writing the data to target REF_METER_DATA_CDR
# MAGIC -- Transform Schema REF_METER_DATA_CDR --AEMO (NEM) recognised Holidays
# MAGIC -- ** ------------------------------------------------------
# MAGIC MERGE INTO $deltalake_schema.REF_METER_DATA_CDR as Target
# MAGIC USING HOL_CDR
# MAGIC ON `Holiday Code`= TYP 
# MAGIC AND `Holiday When`= REQ_BY
# MAGIC 
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET FRM_DT= Null,
# MAGIC TO_DT = Null,
# MAGIC DLVRY_DESC = `Holiday Description`,
# MAGIC ROW_INSERT_DTM=current_timestamp()
# MAGIC 
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT(REQ_BY,TYP,DLVRY_DESC,FRM_DT,TO_DT,SRC_YR,ROW_INSERT_DTM) 
# MAGIC VALUES(`Holiday When`,`Holiday Code`,`Holiday Description`,NULL,NULL,`Year`,current_timestamp())

# COMMAND ----------

#-----------------------------------------------------------------
# Generating parquet files- REF_METER_DATA_CDR
#-----------------------------------------------------------------

df = sqlContext.sql("SELECT * FROM " + deltalake_schema + ".ref_meter_data_cdr")
output_path=parquetfilepath + "ref_meter_data_cdr"
df.write.format("parquet").mode('overwrite').save(output_path)