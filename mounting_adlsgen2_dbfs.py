# Databricks notebook source
df_cust = spark.read.format('parquet').load('dbfs:/mnt/adls_dbfs_mount/data/customer_data.parquet')
df_cust.printSchema

# COMMAND ----------

df_orders = spark.read.format('parquet').load('dbfs:/mnt/adls_dbfs_mount/data/orders_data.parquet')
df_orders.printSchema()

# COMMAND ----------

df_prod = spark.read.format('parquet').load('dbfs:/mnt/adls_dbfs_mount/data/products_data.parquet')
df_prod.printSchema()

# COMMAND ----------

display(df_prod)

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
display(df_cust.distinct().select(col("customer_id"),col("first_name")))

# COMMAND ----------

display(df_cust)

# COMMAND ----------

display(df_orders)

# COMMAND ----------

display(df_cust.alias("A").join(df_orders.alias("B"),col("A.customer_id") == col("B.customer_id"),"left").filter(col("B.customer_id").isNull()))

# COMMAND ----------

display(df_orders)

# COMMAND ----------

df_orders = df_orders.withColumn(
    "customer_id",
    when(col("order_id") == 7, 3)  # Set customer_id to 3 when order_id is 7
    .otherwise(col("customer_id"))  # Keep the original customer_id for other rows
)
display(df_orders)           

# COMMAND ----------

df_orders = df_orders.withColumn("product_id",when(col("order_id") == 3,1).when(col("order_id") == 4,2).when(col("order_id") == 6,2).when(col("order_id") == 7,3).otherwise(None))

# COMMAND ----------

display(df_orders)

# COMMAND ----------

df_joined_orders_prod = df_orders.alias("A").join(df_prod.alias("B"),col("A.product_id") == col("B.product_id"),"inner")

# COMMAND ----------

df_grouped = df_joined_orders_prod.groupBy("A.customer_id").agg(collect_set("B.product_name").alias("products"))
display(df_grouped)

# COMMAND ----------

df_result = df_grouped.filter(array_contains(col("products"),"Laptop") & array_contains(col("products"),"Smartphone") & ~array_contains(col("products"),"Headphones"))

# COMMAND ----------

dbutils.fs.ls("/mnt/adls_dbfs_mount/")

# COMMAND ----------

container_name = "source"
account_name = "databricsksdestorage"
output_path = "abfss://source@databricsksdestorage.dfs.core.windows.net/output/df_orders_output.parquet"

# COMMAND ----------

df_result.write.format('parquet').mode('overwrite').option("path","/mnt/adls_dbfs_mount/output/").save()
