# Databricks notebook source
# MAGIC %run /Workspace/Users/mayurisaikia001@outlook.com/notebooks/basic_functions

# COMMAND ----------

from pyspark.sql.functions import col, max, when, current_timestamp, to_timestamp, coalesce, to_date, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType, DateType,TimestampType
from datetime import datetime
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC Mounting the two containers silver and gold

# COMMAND ----------

storageAccountName = 'proje2e'
list_containers = ['gold-partnersales','silver-partnersales']
get_mount_point(storageAccountName, list_containers);

# COMMAND ----------

# dbutils.fs.ls("/mnt/silver-partnersales/sales/delta/")

# COMMAND ----------

# MAGIC %md
# MAGIC Getting the CertificationLatestVersion file into a dataframe

# COMMAND ----------

silver_certificationLatestVersion=spark.read.format('delta').option('header',True).load('dbfs:/mnt/silver-partnersales/sales/delta/certificationLatestVersion')
# display(silver_certificationLatestVersion.limit(3))


# COMMAND ----------

silver_certificationPassDateLatest=spark.read.format('delta').option('header',True).load('dbfs:/mnt/silver-partnersales/sales/delta/certificationPassDateLatest')
# display(silver_certificationPassDateLatest.limit(3))

# COMMAND ----------

silver_partnerlicensesales=spark.read.format('delta').option('header',True).load('dbfs:/mnt/silver-partnersales/sales/delta/partnerlicensesales')
# display(silver_partnerlicensesales.limit(3))

# COMMAND ----------

silver_newsSentiment=spark.read.format('delta').option('header',True).load('dbfs:/mnt/silver-partnersales/sales/delta/newsSentiment')
# display(silver_newsSentiment.limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC Let's segregate the 4 silver (blue) delta tables into the new tables - Dimension tables (11 pink) & Fact tables (3 orange) as in diagram below

# COMMAND ----------

# MAGIC %md
# MAGIC ![](/Workspace/Users/mayurisaikia001@outlook.com/images/silver-gold tables.PNG)

# COMMAND ----------

# MAGIC %md
# MAGIC ![](/Workspace/Users/mayurisaikia001@outlook.com/images/silver-gold tables1.PNG)

# COMMAND ----------

# MAGIC %md
# MAGIC All Dimension tables creation and saving in delta table mode

# COMMAND ----------

gold_dim_location = silver_certificationPassDateLatest.select('region', 'country', 'state').distinct().withColumn('location_id', monotonically_increasing_id()+1).select('location_id','region','country','state')
gold_dim_location.write.format('delta').mode('overwrite').save('dbfs:/mnt/gold-partnersales/sales/delta/gold_dim_location')

# COMMAND ----------

silver_certificationPassDateLatest.printSchema()

# COMMAND ----------

gold_dim_partner = silver_certificationPassDateLatest.select('partner_company_id','partner_company','partner_id','partner_first_name','partner_last_name','tgt_job_role','multiple_cert_holder','region','country','state').distinct()

# display(gold_dim_partner)

# COMMAND ----------

gold_dim_partner = gold_dim_partner.join(gold_dim_location, on = ['region','country','state'], how = 'left')


# COMMAND ----------

gold_dim_partner = gold_dim_partner.drop('region','country','state')
gold_dim_partner.write.format('delta').mode('overwrite').save('dbfs:/mnt/gold-partnersales/sales/delta/gold_dim_partner')

# COMMAND ----------

gold_dim_ise = silver_certificationPassDateLatest.select('ise_id','ise').distinct()
gold_dim_ise.write.format('delta').mode('overwrite').save('dbfs:/mnt/gold-partnersales/sales/delta/gold_dim_ise')

# COMMAND ----------

gold_dim_ipe = silver_certificationPassDateLatest.select('ipe_id','ipe').distinct()
gold_dim_ipe.write.format('delta').mode('overwrite').save('dbfs:/mnt/gold-partnersales/sales/delta/gold_dim_ipe')

# COMMAND ----------

gold_dim_industry = silver_certificationPassDateLatest.select('industry').distinct().withColumn('industry_id',monotonically_increasing_id()+1)
# display(gold_dim_industry)
gold_dim_industry.write.format('delta').mode('overwrite').save('dbfs:/mnt/gold-partnersales/sales/delta/gold_dim_industry')

# COMMAND ----------

gold_dim_brand = silver_certificationPassDateLatest.select('brand').distinct().withColumn('brand_uk',monotonically_increasing_id()+1)
# display(gold_dim_brand)
gold_dim_brand.write.format('delta').mode('overwrite').save('dbfs:/mnt/gold-partnersales/sales/delta/gold_dim_brand')

# COMMAND ----------

gold_dim_job_role = silver_certificationPassDateLatest.select('tgt_job_role').distinct().withColumn('job_role_id',monotonically_increasing_id()+1).withColumnRenamed('tgt_job_role', 'job_role')
# display(gold_dim_job_role)
gold_dim_job_role.write.format('delta').mode('overwrite').save('dbfs:/mnt/gold-partnersales/sales/delta/gold_dim_job_role')

# COMMAND ----------

from pyspark.sql.functions import split
dim_cert = silver_certificationLatestVersion.select('certification_id','certification_name','latest_version','last_copied').distinct().withColumn('certification_uk',monotonically_increasing_id()+1).withColumn('brand', split(col('certification_name'), '-')[1]).join(gold_dim_brand, on='brand').drop('brand')
gold_dim_certification = dim_cert.select('certification_uk','certification_id','certification_name','brand_uk','latest_version','last_copied')
gold_dim_certification.write.format('delta').mode('overwrite').save('dbfs:/mnt/gold-partnersales/sales/delta/gold_dim_certification')


# COMMAND ----------

gold_dim_license = silver_partnerlicensesales.select('license_type').distinct().withColumn('license_id',monotonically_increasing_id()+1)
gold_dim_license.write.format('delta').mode('overwrite').save('dbfs:/mnt/gold-partnersales/sales/delta/gold_dim_license')

# COMMAND ----------

gold_dim_sentiment = silver_newsSentiment.select('sentiment').distinct().withColumn('sentiment_id',monotonically_increasing_id()+1)
gold_dim_sentiment.write.format('delta').mode('overwrite').save('dbfs:/mnt/gold-partnersales/sales/delta/gold_dim_sentiment')
# display(gold_dim_sentiment)

# COMMAND ----------

silver_partnerlicensesales.printSchema()

# COMMAND ----------

gold_fact_sales1 = silver_partnerlicensesales.join(gold_dim_certification, on = ["certification_id","certification_name"], how = 'left').select("uk_id","partner_company_id","partner_id","certification_uk","industry","ise_id","ipe_id","year_name","month","license_type","sales", silver_partnerlicensesales.last_copied).join(gold_dim_license, on = ["license_type"], how= 'left').select("uk_id","partner_company_id","partner_id","certification_uk","industry","ise_id","ipe_id","year_name","month","license_id","sales","last_copied")

gold_fact_sales = gold_fact_sales1.join(gold_dim_industry, on = "industry", how = 'left').select("uk_id","partner_company_id","partner_id","certification_uk","industry_id","ise_id","ipe_id","year_name","month","license_id","sales","last_copied")
gold_fact_sales.write.format('delta').mode('overwrite').option('overwriteSchema',True).save('dbfs:/mnt/gold-partnersales/sales/delta/gold_fact_sales')

# COMMAND ----------

# display(gold_fact_sales)

# COMMAND ----------

gold_fact_certificationpassdatelatest = silver_certificationPassDateLatest.alias('a')\
  .join(gold_dim_certification.alias('b'), on = ["certification_id","certification_name"], how = 'left')\
    .select('b.certification_uk','b.brand_uk','industry','ise_id','ipe_id','partner_id','pass_date','a.last_copied')\
      .join(gold_dim_industry.alias('c'), on = "industry", how = 'left')\
      .select('partner_id','certification_uk','brand_uk','c.industry_id','ise_id','ipe_id','pass_date','last_copied')
gold_fact_certificationpassdatelatest.write.format('delta').mode('overwrite').save('dbfs:/mnt/gold-partnersales/sales/delta/gold_fact_certificationpassdatelatest')
# display(gold_fact_certificationpassdatelatest.limit(4))


# COMMAND ----------

from pyspark.sql.functions import min, max, expr, to_date, col, year, month, day, quarter
min_date = '2016-01-01'
max_date = '2022-12-31'

# Calculate the date difference first
date_diff = spark.sql(f"""
SELECT datediff(to_date('{max_date}'), to_date('{min_date}')) + 1 as diff
""").collect()[0]["diff"]

print('min_date :',min_date, ';', 'max_date :',max_date, ';', 'date_diff :', date_diff)

# COMMAND ----------

from pyspark.sql.types import StringType

# Use the calculated date difference as the argument for spark.range()
date_range_df = spark.range(date_diff)

# Add the 'id' column to date
date_range_df = date_range_df.withColumn("date", expr(f"date_add(to_date('{min_date}'), cast(id as int))"))
date_range_df = date_range_df.withColumn("year", year(col("date")).cast(StringType()))\
                             .withColumn("month", month(col("date")).cast(StringType()))\
                             .withColumn("quarter", quarter(col("date")).cast(StringType()))\
                             .withColumn("day_in_month", day(col("date")).cast(StringType()))
display(date_range_df)
date_range_df.write.format('delta').mode('overwrite').save('dbfs:/mnt/gold-partnersales/sales/delta/gold_dim_date')

# COMMAND ----------

df_sentiment = silver_newsSentiment.alias('a').join(gold_dim_location.alias('b'), on = ['region','country','state'], how = 'left').select('location_id','industry','year_name','month','sentiment','last_copied')\
  .join(gold_dim_industry.alias('c'), on = 'industry', how = 'left').select('location_id','industry_id','year_name','month','sentiment','last_copied')\
    .join(gold_dim_sentiment.alias('d'), on = 'sentiment', how = 'left').select('location_id','industry_id','year_name','month','sentiment_id','last_copied')
display(df_sentiment)
df_sentiment.write.format('delta').mode('overwrite').save('dbfs:/mnt/gold-partnersales/sales/delta/gold_fact_newssentiment')
