# Databricks notebook source
# MAGIC %run /Workspace/Users/mayurisaikia001@outlook.com/notebooks/basic_functions

# COMMAND ----------

from pyspark.sql.functions import col, sum, lower, when

# COMMAND ----------

storageAccountName = 'proje2e'
list_containers = ['gold-partnersales']
get_mount_point(storageAccountName, list_containers);

# COMMAND ----------

sales_df = spark.read.format("delta").option( 'inferSchema',True).load("dbfs:/mnt/gold-partnersales/sales/delta/gold_fact_sales")
sentiment_df = spark.read.format("delta").option( 'inferSchema',True).load("dbfs:/mnt/gold-partnersales/sales/delta/gold_fact_newssentiment")

# Display schemas to check the structure
sales_df.printSchema()
sentiment_df.printSchema()


# COMMAND ----------

dim_partner_df = spark.read.format("delta").option( 'inferSchema',True).load("dbfs:/mnt/gold-partnersales/sales/delta/gold_dim_partner")
dim_partner_df.printSchema()

# COMMAND ----------

certificationpassdatelatest_df = spark.read.format("delta").option( 'inferSchema',True).load("dbfs:/mnt/gold-partnersales/sales/delta/gold_fact_certificationpassdatelatest")

# Display schemas to check the structure
certificationpassdatelatest_df.printSchema()

# COMMAND ----------

sales_location_df = sales_df.alias('a').join(dim_partner_df.alias('b'), on = 'partner_id',how = 'left').select(col('a.*'),col('b.location_id'))

sales_location_df.count()

# COMMAND ----------

sales_df_all = sales_location_df
display(sales_df_all)

# COMMAND ----------

salesagg_df = sales_df_all.groupBy('location_id','industry_id','year_name','month').agg(sum(sales_df_all.sales).alias('sales')).withColumn('month',lower(col('month')))
display(salesagg_df)

# COMMAND ----------

salesagg_sentiment = salesagg_df.alias('a').join(sentiment_df.alias('b'), on = ['location_id','industry_id','year_name','month']).select(col('a.*'),col('b.sentiment_id').cast('string'))
salesagg_sentiment = salesagg_sentiment.withColumn("sentiment", 
                   when(col("sentiment_id") == 1, 1)
                   .when(col("sentiment_id") == 2, 0)
                   .when(col("sentiment_id") == 3, -1)
                   .otherwise(None)).drop('sentiment_id')



# COMMAND ----------

# changing type of year_name to integer
salesagg_num = salesagg_sentiment.withColumn("year", col("year_name").cast("int"))

# COMMAND ----------

# display(salesagg_num.filter(col('sales')==5))

# COMMAND ----------

# MAGIC %md
# MAGIC Let's check distribution of sales column

# COMMAND ----------

import matplotlib.pyplot as plt

# Collecting summary statistics
summary_stats = salesagg_num.select("sales").summary("count", "mean", "stddev", "min", "25%", "50%", "75%", "max").toPandas()

# Displaying summary statistics
display(summary_stats)

# COMMAND ----------

# Collecting sales data for distribution plot
sales_data = salesagg_num.select("sales").rdd.flatMap(lambda x: x).collect()

# Plotting the distribution
plt.figure(figsize=(10, 6))
plt.hist(sales_data, bins=30, edgecolor='k', alpha=0.7)
plt.title('Sales Distribution')
plt.xlabel('Sales')
plt.ylabel('Frequency')
plt.grid(True)


# COMMAND ----------

from pyspark.sql.functions import format_number

# mean_sales = round(float(salesagg_num.select("sales").summary("mean").collect()[0][1]),2)
# stddev_sales = round(float(salesagg_num.select("sales").summary("stddev").collect()[0][1]),2)

# lower_threshold_sales = round(mean_sales - stddev_sales,2)
# upper_threshold_sales = round(mean_sales + stddev_sales,2)

lower_threshold_sales = 200
upper_threshold_sales = 1000
print ('lower_threshold_sales : '+ str(lower_threshold_sales) )
print ('upper_threshold_sales : '+ str(upper_threshold_sales) )
# print ('mean_sales : '+ str(mean_sales) )

# COMMAND ----------

# MAGIC %md
# MAGIC Logistic Regression

# COMMAND ----------

# MAGIC %md
# MAGIC Sales Classification Logic
# MAGIC
# MAGIC Low: If sales < lower_threshold_sales
# MAGIC
# MAGIC Moderate: If lower_threshold_sales ≤ sales ≤ upper_threshold_sales
# MAGIC
# MAGIC High: If sales > upper_threshold_sales

# COMMAND ----------

from pyspark.ml.feature import StringIndexer, OneHotEncoder , VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline

# COMMAND ----------

df = salesagg_num

# COMMAND ----------

# StringIndexer for 'location_id' and 'industry_id'
location_indexer = StringIndexer(inputCol="location_id", outputCol="location_indexed")
industry_indexer = StringIndexer(inputCol="industry_id", outputCol="industry_indexed")
month_indexer = StringIndexer(inputCol="month", outputCol="month_indexed") #since month is a cyclic value and Dec 12 is not necessarily greater than Jan 1

# OneHotEncoder for 'location_id', 'industry_id'and 'month_id'
location_encoder = OneHotEncoder(inputCol="location_indexed", outputCol="location_onehot")
industry_encoder = OneHotEncoder(inputCol="industry_indexed", outputCol="industry_onehot")
month_encoder = OneHotEncoder(inputCol="month_indexed", outputCol="month_onehot")


# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Assemble all features into a single vector

# COMMAND ----------

feature_columns = ["industry_onehot", "location_onehot", "month_onehot", "year", "sentiment"]
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")

# COMMAND ----------

# MAGIC %md
# MAGIC Creating a pipeline

# COMMAND ----------

pipeline = Pipeline(stages=[industry_indexer, industry_encoder,
                            location_indexer, location_encoder, 
                            month_indexer, month_encoder, assembler])

# COMMAND ----------

# MAGIC %md
# MAGIC Fitting data to the model and transformation of the data

# COMMAND ----------

# for stage in pipeline.getStages(): 
#   if hasattr(stage, 'setOutputCol'): stage.setOutputCol(stage.getOutputCol() + "_new")

df_transformed = pipeline.fit(df).transform(df) 
# display(df_transformed)

# COMMAND ----------

df_transformed = df_transformed.withColumn(
    "sales_classification",
    when(col("sales") < lower_threshold_sales, 0)  # Low sales
    .when((col("sales") >= lower_threshold_sales) & (col("sales") <= upper_threshold_sales), 1)  # Moderate sales
    .otherwise(2)  # High sales
)

# COMMAND ----------

count_sales_classification = df_transformed.groupBy('sales_classification').count().withColumn('sales_classification', \
    when(col('sales_classification') == 0, 'Low')\
        .when(col('sales_classification') == 1, 'Moderate')\
            .otherwise('High'))

count_sales_classification.display()

# COMMAND ----------

count_sales_sentiment_wise = df_transformed.groupBy('sentiment').count().withColumn('sentiment', \
    when(col('sentiment') == -1, 'Negative sentiment')\
        .when(col('sentiment') == 0, 'Neutral Sentiment')\
            .when(col('sentiment') == 1, 'Positive Sentiment').otherwise('No sentiment'))

count_sales_sentiment_wise.display()

# COMMAND ----------

# MAGIC %md
# MAGIC We train the Logistic Regression model

# COMMAND ----------

lr = LogisticRegression(featuresCol="features", labelCol="sales_classification")
lr_model = lr.fit(df_transformed)

# COMMAND ----------

# predictions = lr_model.transform(df_transformed)
# display(predictions.limit(100))

# COMMAND ----------

# MAGIC %md
# MAGIC The sample dataset isn't really genuine data and hence not accurate. Still if we want to check for correlation between sentiment and sales values ...
# MAGIC ##### With real data, we would expect to see some positive correlation between sentiment and sales or else weak correlation. But with this dataset, correlation is -0.007

# COMMAND ----------

from pyspark.sql.functions import corr

# Calculate correlation between sentiment and sales when sales is high
correlation = df_transformed.select(corr("sentiment", "sales")).collect()[0][0]

print(f"Correlation between sentiment and sales: {correlation}")  # -0.00718812562519913

# Convert to Pandas DataFrame for plotting
df_pandas = df_transformed.select("sentiment", "sales").toPandas()

plt.figure(figsize=(10, 6))
plt.scatter(df_pandas['sentiment'], df_pandas['sales'], alpha=0.5)
plt.title('Correlation between Sentiment and Sales :' + str(round(correlation,4)))
plt.xlabel('Sentiment')
plt.ylabel('Sales')
plt.grid(True)
plt.show()
