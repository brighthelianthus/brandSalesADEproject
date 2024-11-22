# Databricks notebook source
# MAGIC %run /Workspace/Users/mayurisaikia001@outlook.com/notebooks/basic_functions

# COMMAND ----------

from pyspark.sql.functions import col, max, when, current_timestamp, to_timestamp, coalesce, to_date
from pyspark.sql.types import StructType, StructField, StringType, DateType,TimestampType, IntegerType
from datetime import datetime
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC Mounting the Bronze container and silver container

# COMMAND ----------

storageAccountName = 'proje2e'
list_containers = ['bronze-partnersales','silver-partnersales']
get_mount_point(storageAccountName, list_containers);

# COMMAND ----------

dbutils.fs.ls("/mnt/bronze-partnersales/sales")

# COMMAND ----------

# MAGIC %md
# MAGIC Getting the partnerlicensesales file into a dataframe

# COMMAND ----------

partnerlicensesales =spark.read.format('parquet').option('header',True).load('dbfs:/mnt/bronze-partnersales/sales/partnerlicensesales')
# display(partnerlicensesales)

# COMMAND ----------

partnerlicensesales.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC To prepare for unpivoting, let's separate out columns that are not related to our pivot operation, also keeping key columns. We will later on make an inner join and add to final dataframe this separate dataframe

# COMMAND ----------

partnerlicensesales_separated = partnerlicensesales.select("partner_id","certification_id","certification_name","partner_company_id","industry","ise_id","ipe_id","uk_id","year_name","last_copied")

# COMMAND ----------

from pyspark.sql.functions import expr

# Select the required columns
columns = ["uk_id", "certification_id", "certification_name","year_name", "industry", "ise_id", "ipe_id", "partner_id", "partner_company_id"] + [f"{month}_{license_type}" for month in ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"] for license_type in ["Standard", "Professional", "Premium"]]
partnerlicensesales_selected = partnerlicensesales.select(columns)

# Unpivot the columns
unpivot_expr = "stack(36, " + ", ".join([f"'{month}', '{license_type}', {month}_{license_type}" for month in ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"] for license_type in ["Standard", "Professional", "Premium"]]) + ") as (month, license_type, sales)"
partnerlicensesales_unpivoted = partnerlicensesales_selected.select("uk_id", "certification_id", "certification_name", "year_name","industry",  "ise_id", "ipe_id", "partner_id", "partner_company_id", expr(unpivot_expr))

# display(partnerlicensesales_unpivoted.filter(col("year_name") == "2022").filter(col("month") == "Apr").filter(col("license_Type") == "Premium"))
partnerlicensesales_unpivoted.count()

# COMMAND ----------

partnerlicensesales_unpivoted_df = partnerlicensesales_separated.join(partnerlicensesales_unpivoted, on = ["uk_id", "certification_id","certification_name","year_name","partner_id","industry", "ise_id","ipe_id","partner_company_id"], how = 'right')
partnerlicensesales_unpivoted_df = partnerlicensesales_unpivoted_df.select("uk_id","partner_company_id","partner_id","certification_id","certification_name", "ise_id","ipe_id","year_name","month","license_type","sales","last_copied","industry")

# COMMAND ----------

display(partnerlicensesales_unpivoted_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC Right now, we don't see any duplicates in the dataframe but to avoid any possibility in the future , we drop duplicate records

# COMMAND ----------

partnerlicensesales_unpivoted_df = partnerlicensesales_unpivoted_df.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC All the Data is in String format. We convert the last_copied and latest_version to timestamp format.

# COMMAND ----------

df_partnerlicensesales= partnerlicensesales_unpivoted_df\
                                    .withColumn("sales", col("sales").cast(IntegerType()))\
                                    .withColumn("last_copied", col("last_copied").cast(TimestampType()))

# COMMAND ----------

df_partnerlicensesales.count()

# COMMAND ----------

df_partnerlicensesales.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC ###Writing to silver layer delta table partnerlicensesales

# COMMAND ----------

# MAGIC %md
# MAGIC step 1: Mounting the Silver container done above

# COMMAND ----------

# MAGIC %md
# MAGIC step 2: if deltable already exists, take it into a variable, else create an empty delta table into the silver container

# COMMAND ----------

path_silver_table = "/mnt/silver-partnersales/sales/delta/partnerlicensesales"

try:
  delta_table = DeltaTable.forPath(spark,path_silver_table)
  print("Received existing delta table ")
except:
  # it the table does not exist, then we create one table
  schema=StructType([
    StructField("uk_id",StringType(),True),
    StructField("partner_company_id",StringType(),True),
    StructField("partner_id",StringType(),True),
    StructField("certification_id",StringType(),True),
    StructField("certification_name",StringType(),True),
    StructField("ise_id",StringType(),True),
    StructField("ipe_id",StringType(),True),
    StructField("year_name",StringType(),True),
    StructField("month",StringType(),True),
    StructField("license_type",StringType(),True),
    StructField("sales",IntegerType(),True),
    StructField("last_copied",TimestampType(),True),
    StructField("industry",StringType(),True)
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
df_partnerlicensesales.printSchema()

# COMMAND ----------

print(delta_table.toDF().subtract(df_partnerlicensesales).count())
print(df_partnerlicensesales.subtract(delta_table.toDF()).count())
# if both above values are 0, then both dataframes are identical, no merge must be done

# COMMAND ----------

# MAGIC %md
# MAGIC step 4: compare both source and target dataframes and then perform a merge

# COMMAND ----------

# Compare the incoming DataFrame with the Delta DataFrame
if delta_table.toDF().subtract(df_partnerlicensesales).count() == 0 and \
  df_partnerlicensesales.subtract(delta_table.toDF()).count() == 0:
    # If there is no difference in data, skip the merge
    print("Incoming DataFrame is identical to the Delta table. Skipping merge.")

else: #perform merging 
  delta_table.alias("target")\
    .merge(
      df_partnerlicensesales.alias("source"),
      "target.certification_id = source.certification_id" and
      "target.certification_name = source.certification_name"  and
      "target.partner_id = source.partner_id" and
      "target.partner_company_id = source.partner_company_id" and
      "target.uk_id = source.uk_id" and 
      "target.year_name = source.year_name" and 
      "target.month = source.month" and 
      "target.license_type = source.license_type" and
      "target.ise_id = source.ise_id" and 
      "target.ipe_id = source.ipe_id" and 
      "target.industry = source.industry" 
    )\
      .whenMatchedUpdate(
        condition = "target.last_copied < source.last_copied",       
        set ={
          "target.last_copied": "source.last_copied",
          "target.sales": "source.sales"
        }
      )\
      .whenNotMatchedInsert(
        values = {
          "target.certification_id" : "source.certification_id" ,
          "target.certification_name" : "source.certification_name"  ,
          "target.partner_id" : "source.partner_id",
          "target.partner_company_id" : "source.partner_company_id",
          "target.uk_id" : "source.uk_id" ,
          "target.year_name" : "source.year_name" ,
          "target.month" : "source.month" ,
          "target.license_type" : "source.license_type" ,
          "target.ise_id" : "source.ise_id" ,
          "target.ipe_id" : "source.ipe_id" ,
          "target.last_copied": "source.last_copied" ,
          "target.sales": "source.sales",
          "target.industry": "source.industry"}
      )\
      .execute()


# COMMAND ----------

# MAGIC %md
# MAGIC Let's try to find out the count of distinct records in target delta table

# COMMAND ----------

df_delta_table = DeltaTable.forPath(spark,path_silver_table).toDF()
display(df_delta_table.distinct().count())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Check list of Delta tables in silver container

# COMMAND ----------

dbutils.fs.ls("/mnt/silver-partnersales/sales/delta/")
