# Databricks notebook source
df_order = spark.read.csv("/Volumes/workspace/default/olist_raw_data/olist_orders_dataset.csv", header=True, inferSchema=True)
#df_order.display()

# COMMAND ----------

from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, DoubleType, TimestampType)

orders_schema = StructType([
StructField("order_id", StringType(), False),
StructField("customer_id", StringType(), False),
StructField("order_status", StringType(), True),
StructField("order_purchase_timestamp", TimestampType(), True),
StructField("order_approved_at", TimestampType(), True),
StructField("order_delivered_carrier_date", TimestampType(), True),
StructField("order_delivered_customer_date", TimestampType(), True),
StructField("order_estimated_delivery_date", TimestampType(), True)

])
customers_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_unique_id", StringType(), False),
    StructField("customer_zip_code_prefix", StringType(), True),
    StructField("customer_city", StringType(), True),
    StructField("customer_state", StringType(), True)
])
order_items_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("order_item_id", IntegerType(), True),
    StructField("product_id", StringType(), False),
    StructField("seller_id", StringType(), False),
    StructField("shipping_limit_date", TimestampType(), True),
    StructField("price", DoubleType(), True),
    StructField("freight_value", DoubleType(), True)
])
payments_schema =StructType([
    StructField("order_id", StringType(), False),
    StructField("payment_sequential", IntegerType(), True),
    StructField("payment_type", StringType(), True),
    StructField("payment_installments", IntegerType(), True),
    StructField("payment_value", DoubleType(), True)
])
reviews_schema = StructType([
    StructField("review_id", StringType(), False),
    StructField("order_id", StringType(), False),
    StructField("review_score", IntegerType(), True),
    StructField("review_comment_title", StringType(), True),
    StructField("review_comment_message", StringType(), True),
    StructField("review_creation_date", TimestampType(), True),
    StructField("review_answer_timestamp", TimestampType(), True)
])
products_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_category_name", StringType(), True),
    StructField("product_name_lenght", IntegerType(), True),
    StructField("product_description_lenght", IntegerType(), True),
    StructField("product_photos_qty", IntegerType(), True),
    StructField("product_weight_g", IntegerType(), True),
    StructField("product_length_cm", IntegerType(), True),
    StructField("product_height_cm", IntegerType(), True),
    StructField("product_width_cm", IntegerType(), True)
])
sellers_schema = StructType([
    StructField("seller_id", StringType(), False),
    StructField("seller_zip_code_prefix", StringType(), True),
    StructField("seller_city", StringType(), True),
    StructField("seller_state", StringType(), True)
])
geolocation_schema = StructType([
    StructField("geolocation_zip_code_prefix", StringType(), False),
    StructField("geolocation_lat", DoubleType(), True),
    StructField("geolocation_lng", DoubleType(), True),
    StructField("geolocation_city", StringType(), True),
    StructField("geolocation_state", StringType(),  True)
])

translation_schema = StructType([
    StructField("product_category_name", StringType(), False),
    StructField("product_category_name_english", StringType(), True)
])


# COMMAND ----------

file_names = [
    "olist_orders_dataset",
    "olist_customers_dataset",
    "olist_order_items_dataset",
    "olist_order_payments_dataset",
    "olist_order_reviews_dataset",
    "olist_products_dataset",
    "olist_sellers_dataset",
    "olist_geolocation_dataset",
    "product_category_name_translation"
]

base_path ="/Volumes/workspace/default/olist_raw_data/"

bronze_dfs = {}  
schemas = {
    "olist_orders_dataset": orders_schema,
    "olist_customers_dataset": customers_schema,
    "olist_order_items_dataset": order_items_schema,
    "olist_order_payments_dataset": payments_schema,
    "olist_order_reviews_dataset": reviews_schema,
    "olist_products_dataset": products_schema,
    "olist_sellers_dataset": sellers_schema,
    "olist_geolocation_dataset": geolocation_schema,
    "product_category_name_translation": translation_schema
    
}

bronze_dfs = {}

for name in file_names:
    file_path = base_path + name + ".csv"
    bronze_dfs[name] = spark.read.csv(file_path, header=True, schema=schemas[name])
    print(f"Loaded {name} - Row Count: {bronze_dfs[name].count()}")               #data is in memory only

'''for name in file_names:
    file_path = base_path+name+".csv"
    bronze_dfs[name] = spark.read.csv(file_path, header=True, inferSchema=True)
    print(f"Loaded {name} - Row Count: {bronze_dfs[name].count()}")'''

# COMMAND ----------

#to make our data persistent I'll write data into a Delta Lake

bronze_output_path = "/Volumes/workspace/default/olist_raw_data/bronze/"

for name, df in bronze_dfs.items():
    output_path = bronze_output_path + name
    df.write.format("delta").mode("overwrite").option("overwriteschema", "true").save(output_path)
    print(f"Written {name} to Bronze Delta Table")

# COMMAND ----------

#check if they are readable

for name in file_names:
    bronze_path = bronze_output_path+name
    df = spark.read.format("delta").load(bronze_path)
    print(f"Read {name} from Bronze Delta Table - Row Count: {df.count()} | Columns: {len(df.columns)}")
#

# COMMAND ----------

spark.sql("DESCRIBE HISTORY delta.`/Volumes/workspace/default/olist_raw_data/bronze/olist_customers_dataset`").display()

# COMMAND ----------

for name in file_names:
    bronze_path = bronze_output_path + name
    df = spark.read.format("delta").load(bronze_path)
    print(f"Read {name} from Bronze Delta Table - Row Count: {df.count()} | Columns: {len(df.columns)}")

# COMMAND ----------

