# Databricks notebook source
# MAGIC %run /Workspace/Users/mayurisaikia001@outlook.com/notebooks/basic_functions

# COMMAND ----------

from pyspark.sql.functions import col, max, when, current_timestamp, to_timestamp, coalesce, to_date
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
# MAGIC Getting the newssentiment file into a dataframe

# COMMAND ----------

newssentiment=spark.read.format('parquet').option('header',True).load('dbfs:/mnt/bronze-partnersales/sales/newssentiment')
display(newssentiment)


# COMMAND ----------

display(newssentiment.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC All the Data is in String format. We convert the last_copied to timestamp format.

# COMMAND ----------

clean_newssentiment= newssentiment.withColumn("last_copied", col("last_copied").cast(TimestampType()))
clean_newssentiment = clean_newssentiment.toDF(*[col_name.lower() for col_name in clean_newssentiment.columns])

# COMMAND ----------

df_newssentiment = clean_newssentiment.unpivot(["region","country","state","industry","year_name","last_copied"], ["jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"],"month","sentiment")

# COMMAND ----------

df_newssentiment.orderBy("region","country","state","industry","year_name","month").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Writing to silver layer delta table newsSentiment

# COMMAND ----------

# MAGIC %md
# MAGIC step 1: Mounting the Silver container done above

# COMMAND ----------

# MAGIC %md
# MAGIC step 2: if deltable already exists, take it into a variable, else create an empty delta table into the silver container

# COMMAND ----------

path_silver_table = "/mnt/silver-partnersales/sales/delta/newsSentiment"

try:
  delta_table = DeltaTable.forPath(spark,path_silver_table)
  print("Received existing delta table ")
except:
  # it the table does not exist, then we create one table
  schema=StructType([
    StructField("region",StringType(),True),
    StructField("country",StringType(),True),
    StructField("state",StringType(),True),
    StructField("industry",StringType(),True),
    StructField("year_name",StringType(),True),
    StructField("last_copied",TimestampType(),True),
    StructField("month",StringType(),True),
    StructField("sentiment",StringType(),True)
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
df_newssentiment.printSchema()

# COMMAND ----------

# print(delta_table.toDF().subtract(df_newssentiment).count())
# print(df_newssentiment.subtract(delta_table.toDF()).count())
# if both above values are 0, then both dataframes are identical, no merge must be done

# COMMAND ----------

# MAGIC %md
# MAGIC step 4: compare both source and target dataframes and then perform a merge

# COMMAND ----------

# Compare the incoming DataFrame with the Delta DataFrame
if delta_table.toDF().subtract(df_newssentiment).count() == 0 and \
  df_newssentiment.subtract(delta_table.toDF()).count() == 0:
    # If there is no difference in data, skip the merge
    print("Incoming DataFrame is identical to the Delta table. Skipping merge.")

else: #perform merging 
  delta_table.alias("target")\
    .merge(
      df_newssentiment.alias("source"),
      "target.region = source.region" and
      "target.country = source.country" and  
      "target.state = source.state" and
      "target.country = source.country" and  
      "target.year_name = source.year_name" and
      "target.month = source.month"
    )\
      .whenMatchedUpdate(
        condition = "target.last_copied < source.last_copied",       
        set ={
          "target.last_copied": "source.last_copied",
          "target.sentiment": "source.sentiment"
        }
      )\
      .whenNotMatchedInsert(
        values = {
        "target.region": "source.region" ,
        "target.country": "source.country" ,
        "target.state": "source.state" ,
        "target.industry" : "source.industry",
        "target.year_name" : "source.year_name",
        "target.month" : "source.month",
        "target.sentiment" : "source.sentiment",
        "target.last_copied" : "source.last_copied"}
      )\
      .execute()


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Check list of Delta tables in silver container

# COMMAND ----------

dbutils.fs.ls("/mnt/silver-partnersales/sales/delta/")
