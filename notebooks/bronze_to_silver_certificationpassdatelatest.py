# Databricks notebook source
# MAGIC %run /Workspace/Users/mayurisaikia001@outlook.com/notebooks/basic_functions

# COMMAND ----------

from pyspark.sql.functions import col, max, when, current_timestamp, to_timestamp, coalesce
from pyspark.sql.types import StructType, StructField, StringType, DateType, BooleanType, TimestampType, IntegerType
from datetime import datetime
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC Mounting the Bronze container

# COMMAND ----------

storageAccountName = 'proje2e'
list_containers = ['bronze-partnersales','silver-partnersales']
get_mount_point(storageAccountName, list_containers);

# COMMAND ----------

dbutils.fs.ls("/mnt/bronze-partnersales/sales")

# COMMAND ----------

# MAGIC %md
# MAGIC Getting the partnerCertificationPassDate file into a dataframe

# COMMAND ----------

certificationpassdate=spark.read.format('parquet').option('header',True).load('dbfs:/mnt/bronze-partnersales/sales/partnerCertificationPassDate')
# display(certificationpassdate.filter(col('certification_id')=='CERT191').filter(col('certification_name')=='Energy Management Foundations-Catia').filter( col('partner_id')=='STELL97437'))

# COMMAND ----------

# MAGIC %md
# MAGIC Since there could be multiple entries , we drop duplicate records

# COMMAND ----------

certificationpassdate = certificationpassdate.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC For those having multiple Pass_Dates 

# COMMAND ----------

certificationpassdate_distinct = certificationpassdate.groupBy('certification_id','certification_name','partner_id','industry','ise_id','ipe_id').count()
# display(certificationpassdate_distinct.filter(col('count')> 1))

# COMMAND ----------


# display(
#   certificationpassdate.filter(col('certification_id')=='CERT131').filter(col('certification_name')=='Design Solutions Foundations-Netvibes').filter( col('partner_id')=='CORE57204')
#   .union(
#   certificationpassdate.filter(col('certification_id')=='CERT191').filter(col('certification_name')=='Energy Management Foundations-Catia').filter( col('partner_id')=='STELL97437')
#   )
#   .union(
#   (certificationpassdate.filter(col('certification_id')=='CERT070').filter(col('certification_name')=='Regulatory Compliance Applications-3Dvia').filter( col('partner_id')=='ENVI98873')))
# )

# COMMAND ----------

# MAGIC %md
# MAGIC Since there are multiple records for each (partner id, certification id, certification name, industry, ise, ipe) and varied pass date, we take only the latest of the pass date i.e. the maximum pass date

# COMMAND ----------


certificationpassdate_max = certificationpassdate.groupBy('certification_id','certification_name','partner_id','industry','ipe_id','ise_id').agg(max('pass_date').alias('max_pass_date'))


certificationpassdate_max_count = certificationpassdate_max.groupBy('certification_id','certification_name','partner_id').count()
# display(certificationpassdate_max_count.filter(col('count')> 1))

# display(
#   certificationpassdate_max.filter(col('certification_id')=='CERT131').filter(col('certification_name')=='Design Solutions Foundations-Netvibes').filter( col('partner_id')=='CORE57204')
#   .union(
#   certificationpassdate_max.filter(col('certification_id')=='CERT191').filter(col('certification_name')=='Energy Management Foundations-Catia').filter( col('partner_id')=='STELL97437')
#   )
#   .union(
#   (certificationpassdate_max.filter(col('certification_id')=='CERT070').filter(col('certification_name')=='Regulatory Compliance Applications-3Dvia').filter( col('partner_id')=='ENVI98873')))
# )



# COMMAND ----------

# MAGIC %md
# MAGIC We join the resultant dataframe with the original dataframe to retain all records (with max pass_date) and all columns

# COMMAND ----------

certificationpassdatelatest = certificationpassdate.join(certificationpassdate_max,
                                                         (certificationpassdate.Certification_ID == certificationpassdate_max.certification_id) &
                                                         (certificationpassdate.Certification_Name == certificationpassdate_max.certification_name ) &
                                                         (certificationpassdate.Partner_ID == certificationpassdate_max.partner_id)
                                                         &
                                                         (certificationpassdate.Industry == certificationpassdate_max.industry)
                                                         &
                                                         (certificationpassdate.ISE_ID == certificationpassdate_max.ise_id)
                                                         &
                                                         (certificationpassdate.IPE_ID == certificationpassdate_max.ipe_id)
                                                          &
                                                         (certificationpassdate.Pass_Date == certificationpassdate_max.max_pass_date),
                                                         how = 'inner'
                                                         ).drop(certificationpassdate_max.max_pass_date,certificationpassdate_max.partner_id,certificationpassdate_max.certification_name,certificationpassdate_max.certification_id , certificationpassdate_max.industry, certificationpassdate_max.ise_id, 
                                                         certificationpassdate_max.ipe_id)

# COMMAND ----------


# display(
#   certificationpassdatelatest.filter(col('certification_id')=='CERT131').filter(col('certification_name')=='Design Solutions Foundations-Netvibes').filter( col('partner_id')=='CORE57204')
#   .union(
#   certificationpassdatelatest.filter(col('certification_id')=='CERT191').filter(col('certification_name')=='Energy Management Foundations-Catia').filter( col('partner_id')=='STELL97437')
#   )
#   .union(
#   (certificationpassdatelatest.filter(col('certification_id')=='CERT070').filter(col('certification_name')=='Regulatory Compliance Applications-3Dvia').filter( col('partner_id')=='ENVI98873')))
# )

# COMMAND ----------

# display(certificationpassdatelatest.describe())

# COMMAND ----------

# certificationpassdatelatest.summary()

# COMMAND ----------

# MAGIC %md
# MAGIC All the Data is in String format. We convert the last_copied and pass_Date to timestamp format.

# COMMAND ----------

df_certificationpassdatelatest = certificationpassdatelatest\
                                    .withColumn("Pass_Date",col("Pass_Date").cast(TimestampType()))\
                                    .withColumn("last_copied", col("last_copied").cast(TimestampType()))

# COMMAND ----------

df_certificationpassdatelatest.dropDuplicates()

# COMMAND ----------

df_certificationpassdatelatest = df_certificationpassdatelatest.toDF(*[col_name.lower() for col_name in df_certificationpassdatelatest.columns])

# COMMAND ----------

# MAGIC %md
# MAGIC ###Writing to silver layer delta table certificationpassdatelatest

# COMMAND ----------

# MAGIC %md
# MAGIC step 1: Mounting the Silver container using Databricks

# COMMAND ----------

storageAccountName = 'proje2e'
list_containers = ['bronze-partnersales','silver-partnersales']
get_mount_point(storageAccountName, list_containers);

# COMMAND ----------

# MAGIC %md
# MAGIC step 2: if deltable already exists, take it into a variable, else create an empty delta table into the silver container

# COMMAND ----------

path_silver_table = "/mnt/silver-partnersales/sales/delta/certificationPassDateLatest"

try:
  delta_table = DeltaTable.forPath(spark,path_silver_table)
  print("Received existing delta table ")
except:
  # it the table does not exist, then we create one table
  schema=StructType([
    StructField("brand",StringType(),True),
    StructField("partner_company",StringType(),True),
    StructField("partner_company_id",StringType(),True),
    StructField("ipe",StringType(),True),
    StructField("ise",StringType(),True),
    StructField("tgt_job_role",StringType(),True),
    StructField("partner_first_name",StringType(),True),
    StructField("partner_last_name",StringType(),True),
    StructField("multiple_cert_holder",StringType(),True),
    StructField("region",StringType(),True),
    StructField("country",StringType(),False),
    StructField("state",StringType(),False),
    StructField("pass_date",TimestampType(),True),
    StructField("last_copied",TimestampType(),True),
    StructField("certification_id",StringType(),True),
    StructField("certification_name",StringType(),True),
    StructField("partner_id",StringType(),True),
    StructField("industry",StringType(),True),
    StructField("ipe_id",StringType(),True),
    StructField("ise_id",StringType(),True)
     ])
  empty_dataframe = spark.createDataFrame(spark.sparkContext.emptyRDD(),schema)
  empty_dataframe.write.format("delta").mode("overwrite").save(path_silver_table)
  print("Saved Empty Delta Table")
  delta_table = DeltaTable.forPath(spark,path_silver_table)



# COMMAND ----------

# MAGIC %md
# MAGIC step 3: perform optimization

# COMMAND ----------

delta_table.optimize().executeCompaction()
delta_table.vacuum(retentionHours=168)


# COMMAND ----------

# display(delta_table.toDF())
# display(df_certificationpassdatelatest)

# COMMAND ----------

print(delta_table.toDF().subtract(df_certificationpassdatelatest).count())
print(df_certificationpassdatelatest.subtract(delta_table.toDF()).count())
# if both above values are 0, then both dataframes are identical, no merge must be done

# COMMAND ----------

# MAGIC %md
# MAGIC step 4: compare both source and target dataframes and then perform a merge

# COMMAND ----------

# Compare the incoming DataFrame with the Delta DataFrame
if delta_table.toDF().subtract(df_certificationpassdatelatest).count() == 0 and \
  df_certificationpassdatelatest.subtract(delta_table.toDF()).count() == 0:
    # If there is no difference in data, skip the merge
    print("Incoming DataFrame is identical to the Delta table. Skipping merge.")

else: #perform merging 
  delta_table.alias("target")\
    .merge(
      df_certificationpassdatelatest.alias("source"),
      "target.certification_id = source.certification_id" and
      "target.certification_name = source.certification_name" and
      "target.partner_id = source.partner_id" and
      "target.industry = source.industry" and
      "target.ise_id = source.ise_id" and
      "target.ipe_id = source.ipe_id"  
    )\
      .whenMatchedUpdate(
        condition = "target.last_copied < source.last_copied",       
        set ={
          "target.last_copied": "source.last_copied",
          "target.pass_date" : "source.pass_date"
        }
      )\
      .whenNotMatchedInsert(
        values = {
        "target.brand": "source.brand" ,
        "target.certification_id": "source.certification_id" ,
        "target.certification_name": "source.certification_name" ,
        "target.industry":"source.industry" ,
        "target.ise_id":"source.ise_id" ,
        "target.ipe_id":"source.ipe_id" ,
        "target.partner_id":"source.partner_id",
        "target.last_copied" : "source.last_copied",
        "target.pass_date" : "source.pass_date",
        "target.tgt_job_role": "source.tgt_job_role" ,
        "target.multiple_cert_holder":"source.Multiple_Cert_Holder" ,
        "target.ise":"source.ise" ,
        "target.ipe":"source.ipe" ,
        "target.partner_first_name":"source.partner_first_name",
        "target.partner_last_name" : "source.partner_last_name",
        "target.partner_company_id":"source.partner_company_id",
        "target.partner_company" : "source.partner_company",
        "target.region" : "source.region",
        "target.country" : "source.country",
        "target.state" : "source.state"
        }
      )\
      .execute()


# COMMAND ----------

# MAGIC %md
# MAGIC ##### In the case there is a merge conflict. It seems that df_certificationpassdatelatest has multiple records that are trying to update existing records in delta table

# COMMAND ----------

# MAGIC %md
# MAGIC Let's try to find out the count of distinct records in source dataframe

# COMMAND ----------

# display(df_certificationpassdatelatest.groupBy("certification_id","certification_name","industry","ise_id","ipe_id","partner_id").count().filter(col('count') > 1))

# COMMAND ----------

# MAGIC %md
# MAGIC Let's try to find out the count of distinct records in target delta table

# COMMAND ----------


# display(delta_table.toDF().groupBy("certification_id","certification_name","industry","ise_id","ipe_id","partner_id").count().filter(col('count') > 1))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Check list of Delta tables in silver container

# COMMAND ----------

dbutils.fs.ls("/mnt/silver-partnersales/sales/delta/")
