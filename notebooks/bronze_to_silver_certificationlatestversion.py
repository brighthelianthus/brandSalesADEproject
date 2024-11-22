# Databricks notebook source
# MAGIC %run /Workspace/Users/mayurisaikia001@outlook.com/notebooks/basic_functions

# COMMAND ----------

from pyspark.sql.functions import col, max, when, current_timestamp, to_timestamp, coalesce, to_date
from pyspark.sql.types import StructType, StructField, StringType, DateType,TimestampType
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
# MAGIC Getting the CertificationLatestVersion file into a dataframe

# COMMAND ----------

certificationLatestVersion=spark.read.format('parquet').option('header',True).load('dbfs:/mnt/bronze-partnersales/sales/certificationLatestVersion')
#display(certificationLatestVersion)
#display(certificationLatestVersion.filter(col('certification_id')=='CERT191').filter(col('certification_name')=='Energy Management Foundations-Catia')

# COMMAND ----------

# display(certificationLatestVersion)

# COMMAND ----------

# MAGIC %md
# MAGIC Right now, we don't see any duplicates in the dataframe but to avoid any possibility in the future , we drop duplicate records

# COMMAND ----------

certificationLatestVersion = certificationLatestVersion.dropDuplicates()

# COMMAND ----------

# display(certificationLatestVersion.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC All the Data is in String format. We convert the last_copied and latest_version to timestamp format.

# COMMAND ----------

df_certificationLatestVersion= certificationLatestVersion\
                                    .withColumn("latest_version", to_date(col("lastest_version"), "dd-MM-yyyy"))\
                                    .withColumn("last_copied", col("last_copied").cast(TimestampType()))\
                                      .drop("lastest_version")

# COMMAND ----------

# display(df_certificationLatestVersion)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Writing to silver layer delta table certificationLatestVersion

# COMMAND ----------

# MAGIC %md
# MAGIC step 1: Mounting the Silver ADLS container was already done in first `cell`

# COMMAND ----------

# MAGIC %md
# MAGIC step 2: if deltable already exists, take it into a variable, else create an empty delta table into the silver container

# COMMAND ----------

path_silver_table = "/mnt/silver-partnersales/sales/delta/certificationLatestVersion"

try:
  delta_table = DeltaTable.forPath(spark,path_silver_table)
  print("Received existing delta table ")
except:
  # it the table does not exist, then we create one table
  schema=StructType([
    StructField("brand",StringType(),True),
    StructField("certification_id",StringType(),True),
    StructField("certification_name",StringType(),True),
    StructField("last_copied",TimestampType(),True),
    StructField("latest_version",DateType(),True)
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

delta_table.toDF().printSchema()
df_certificationLatestVersion.printSchema()

# COMMAND ----------

print(delta_table.toDF().subtract(df_certificationLatestVersion).count())
print(df_certificationLatestVersion.subtract(delta_table.toDF()).count())
# if both above values are 0, then both dataframes are identical, no merge must be done

# COMMAND ----------

# MAGIC %md
# MAGIC step 4: compare both source and target dataframes and then perform a merge

# COMMAND ----------

# Compare the incoming DataFrame with the Delta DataFrame
if delta_table.toDF().subtract(df_certificationLatestVersion).count() == 0 and \
  df_certificationLatestVersion.subtract(delta_table.toDF()).count() == 0:
    # If there is no difference in data, skip the merge
    print("Incoming DataFrame is identical to the Delta table. Skipping merge.")

else: #perform merging 
  delta_table.alias("target")\
    .merge(
      df_certificationLatestVersion.alias("source"),
      "target.certification_id = source.certification_id" and
      "target.certification_name = source.certification_name"  
    )\
      .whenMatchedUpdate(
        condition = "target.last_copied < source.last_copied",       
        set ={
          "target.last_copied": "source.last_copied",
          "target.latest_version": "source.latest_version"
        }
      )\
      .whenNotMatchedInsert(
        values = {
        "target.brand": "source.brand" ,
        "target.certification_id": "source.certification_id" ,
        "target.certification_name": "source.certification_name" ,
        "target.latest_version" : "source.latest_version",
        "target.last_copied" : "source.last_copied"}
      )\
      .execute()


# COMMAND ----------

# MAGIC %md
# MAGIC ##### In the case there is a merge conflict. It seems that df_certificationLatestVersion has multiple records that are trying to update existing records in delta table

# COMMAND ----------

# MAGIC %md
# MAGIC Let's try to find out the count of distinct records in source dataframe

# COMMAND ----------

# display(df_certificationLatestVersion.groupBy("certification_id","certification_name".count().filter(col('count') > 1))

# COMMAND ----------

# MAGIC %md
# MAGIC Let's try to find out the count of distinct records in target delta table

# COMMAND ----------


# display(delta_table.toDF().groupBy("certification_id","certification_name".count().filter(col('count') > 1))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Check list of Delta tables in silver container

# COMMAND ----------

dbutils.fs.ls("/mnt/silver-partnersales/sales/delta/")
