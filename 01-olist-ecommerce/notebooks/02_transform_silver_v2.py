# Databricks notebook source
from pyspark.sql.functions import (col, lit, when, trim, lower, current_timestamp, lpad, length)
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, DoubleType, TimestampType)
import uuid
#paths
bronze_path = "/Volumes/workspace/default/olist_raw_data/bronze/"
silver_path = "/Volumes/workspace/default/olist_raw_data/silver_v2/"
quarantine_path = "/Volumes/workspace/default/olist_raw_data/quarantine_v2/"


#unique id for each pipeline run
batch_id = str(uuid.uuid4())
print(f"Batch id: {batch_id}")

# COMMAND ----------

#inferSchema is expensive (for large files) bcz spark reads data twice even though olist data is not huge still i will perform schema enforcement
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

# COMMAND ----------

#1st table orders
df_orders_raw = spark.read.format("delta").load(bronze_path+"olist_orders_dataset")

print(f"Orders raw count: {df_orders_raw.count()}")

df_orders_raw.printSchema()

# COMMAND ----------

df_orders_clean = df_orders_raw.withColumn("order_status", trim(lower(col("order_status"))))

df_orders_clean.select("order_status").distinct().display()

# COMMAND ----------

statuses = df_orders_clean.select("order_status").distinct().collect()
for row in statuses:
    print(row[0])

# COMMAND ----------

valid_statuses = ["invoiced", "processing", "shipped", "unavailable", "created", "approved", "delivered","canceled"]

#identifying bad records

bad_orders = df_orders_clean.filter(
    (col("order_id").isNull()) |
    (col("customer_id").isNull()) |
    (col("order_purchase_timestamp").isNull()) |
    (~col("order_status").isin(valid_statuses)) |
    (col("order_delivered_customer_date").isNotNull() &
 (col("order_delivered_customer_date") < col("order_purchase_timestamp")))
).withColumn("rejection_reason", 
             when(col("order_id").isNull(), lit("null_order_id"))
             .when(col("customer_id").isNull(), lit("null_customer_id"))
             .when(col("order_purchase_timestamp").isNull(), lit("null_purchase_timestamp"))
             .when(~col("order_status").isin(valid_statuses), lit("invalid_order_status"))
             .otherwise("delivered_before_purchased")
             ).withColumn("rejected_at", current_timestamp()).withColumn("batch_id", lit(batch_id)).withColumn("source_file", lit("olist_orders_dataset")) #audit cols


# COMMAND ----------

good_orders = df_orders_clean.filter(
    (col("order_id").isNotNull()) &
    (col("customer_id").isNotNull()) &
    (col("order_purchase_timestamp").isNotNull()) &
    (col("order_status").isin(valid_statuses)) &
     (
        col("order_delivered_customer_date").isNull() |
        (col("order_delivered_customer_date") >= col("order_purchase_timestamp"))
    )
).withColumn("ingested_at", current_timestamp()).withColumn("batch_id",lit(batch_id)).withColumn("source_file", lit("olist_orders_dataset"))

total = df_orders_clean.count()
good = good_orders.count()
bad = bad_orders.count()

print(f"Total records: {total}")
print(f"Good records: {good}")
print(f"Bad records: {bad}")


# COMMAND ----------

#df_orders_clean.filter(col("order_id").isNull()).count()

# COMMAND ----------

good_orders.select("ingested_at", "batch_id", "source_file").show(5, truncate=False)

# COMMAND ----------

#saving good rows to silver and bad rows to quarantined
good_orders.write.format("delta").mode("overwrite").save(silver_path+"orders")
bad_orders.write.format("delta").mode("overwrite").save(quarantine_path+"orders")

# COMMAND ----------

spark.read.format("delta").load(silver_path + "orders").count()


# COMMAND ----------

spark.read.format("delta").load(quarantine_path + "orders").count()


# COMMAND ----------

customers_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_unique_id", StringType(), False),
    StructField("customer_zip_code_prefix", StringType(), True),
    StructField("customer_city", StringType(), True),
    StructField("customer_state", StringType(), True)
])

# COMMAND ----------

#2nd table customers
df_customers_raw = spark.read.format("delta").load(bronze_path + "olist_customers_dataset")
print(f"Customers raw count: {df_customers_raw.count()}")
df_customers_raw.printSchema()

# COMMAND ----------

df_customers_raw.select("customer_zip_code_prefix").distinct().orderBy("customer_zip_code_prefix").show(10)

# COMMAND ----------

df_customers_clean= df_customers_raw.withColumn("customer_zip_code_prefix", lpad(col("customer_zip_code_prefix").cast(StringType()), 5,"0"))

# COMMAND ----------

df_customers_clean.select("customer_zip_code_prefix").distinct().orderBy("customer_zip_code_prefix").show(10) #handled initial 0s by using lpad and string type

# COMMAND ----------

df_customers_clean= df_customers_clean.withColumn("customer_city", trim(lower(col("customer_city")))).withColumn("customer_state", trim(lower(col("customer_state"))))

# COMMAND ----------

df_customers_clean.select("customer_state").distinct().orderBy("customer_state").show(30) #cleaned states - 2 lettesr - no nulls -  no typo exactly 26 states+1 district

# COMMAND ----------

bad_customers = df_customers_clean.filter(
    (col("customer_id").isNull()) |
    (col("customer_unique_id").isNull()) |
    (length(col("customer_zip_code_prefix")) != 5)
).withColumn("rejection_reason", 
             when(col("customer_id").isNull(), lit("null_customer_id"))
             .when(col("customer_unique_id").isNull(), lit("null_customer_unique_id"))
             .when((length(col("customer_zip_code_prefix")) != 5), lit("invalid_zip_code"))
             .otherwise("unknown_reason")
             ).withColumn("rejected_at", current_timestamp()).withColumn("batch_id", lit(batch_id)).withColumn("source_file", lit("olist_customers_dataset"))


# COMMAND ----------

good_customers = df_customers_clean.filter(
    (col("customer_id").isNotNull()) &
    (col("customer_unique_id").isNotNull()) &
    (length(col("customer_zip_code_prefix"))==5)
).withColumn("ingested_at", current_timestamp()).withColumn("batch_id", lit(batch_id)).withColumn("source_file", lit("olist_customers_dataset"))

# COMMAND ----------

total = df_customers_clean.count()
good = good_customers.count()
bad = bad_customers.count()

print(f"Total: {total}")
print(f"Good: {good}")
print(f"Bad: {bad}")
print(f"Good + Bad = {good + bad}")

# COMMAND ----------

good_customers.write.format("delta").mode("overwrite").save(silver_path + "customers")
bad_customers.write.format("delta").mode("overwrite").save(quarantine_path + "customers")

# COMMAND ----------

print(spark.read.format("delta").load(silver_path + "customers").count())
print(spark.read.format("delta").load(quarantine_path + "customers").count())

# COMMAND ----------

order_items_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("order_item_id", IntegerType(), True),
    StructField("product_id", StringType(), False),
    StructField("seller_id", StringType(), False),
    StructField("shipping_limit_date", TimestampType(), True),
    StructField("price", DoubleType(), True),
    StructField("freight_value", DoubleType(), True)
])

# COMMAND ----------

#3rd table order_items
df_order_items_raw = spark.read.format("delta").load(bronze_path + "olist_order_items_dataset")
print(f"Order items raw count: {df_order_items_raw.count()}")
df_order_items_raw.printSchema()

# COMMAND ----------

df_order_items_raw.filter(col("freight_value") == 0).count()

# COMMAND ----------

bad_order_items = df_order_items_raw.filter(
   (col("order_id").isNull()) |
    (col("product_id").isNull()) |
    (col("seller_id").isNull()) |
    (col("price")<=0) |
    (col("freight_value")<0)
).withColumn("rejection_reason", 
             when(col("order_id").isNull(), lit("null_order_id"))
             .when(col("product_id").isNull(), lit("null_product_id"))
             .when(col("seller_id").isNull(), lit("null_seller_id"))
             .when(col("price")<=0 , lit("invalid_price"))
             .when(col("freight_value")<0, lit("invalid_freight_value"))
             .otherwise("unknown_reason")
             ).withColumn("rejected_at", current_timestamp()).withColumn("batch_id", lit(batch_id)).withColumn("source_file", lit("olist_order_items_dataset"))
             

# COMMAND ----------

good_order_items = df_order_items_raw.filter(
    (col("order_id").isNotNull()) &
    (col("product_id").isNotNull()) &
    (col("seller_id").isNotNull()) &
    (col("price") > 0) &
    (col("freight_value") >= 0)
).withColumn("ingested_at", current_timestamp()) \
 .withColumn("batch_id", lit(batch_id)) \
 .withColumn("source_file", lit("olist_order_items_dataset"))

# COMMAND ----------

total = df_order_items_raw.count()
good = good_order_items.count()
bad = bad_order_items.count()
print(f"Total: {total} | Good: {good} | Bad: {bad} | Good+Bad: {good+bad}")


# COMMAND ----------

good_order_items.write.format("delta").mode("overwrite").save(silver_path + "order_items")
bad_order_items.write.format("delta").mode("overwrite").save(quarantine_path + "order_items")
print(spark.read.format("delta").load(silver_path + "order_items").count())
print(spark.read.format("delta").load(quarantine_path + "order_items").count())


# COMMAND ----------

#4th table payments

df_payments_raw = spark.read.format('delta').load(bronze_path+"olist_order_payments_dataset")
print(f"Payments raw count: {df_payments_raw.count()}")
df_payments_raw.printSchema()

# COMMAND ----------

df_payments_raw.select("payment_type").distinct().show()

# COMMAND ----------

df_payments_clean = df_payments_raw.withColumn("payment_type", trim(lower(col("payment_type"))))
df_payments_clean.filter(col("payment_type") == "not_defined").count()


# COMMAND ----------


bad_payments= df_payments_clean.filter(
    (col("order_id").isNull()) |
    (col("payment_sequential")<1) |
    (col("payment_type")=="not_defined") |
    (col("payment_installments")<1) |
    (col("payment_value")<=0)
).withColumn("rejection_reason", 
             when(col("order_id").isNull(), lit("null_order_id"))
             .when(col("payment_sequential")<1, lit("invalid_payment_sequential"))
             .when(col("payment_type")=="not_defined", lit("invalid_payment_type"))
             .when(col("payment_installments")<1, lit("invalid_payment_installments"))
             .when(col("payment_value")<=0, lit("invalid_payment_value"))
             .otherwise("unknown_reason")
             ).withColumn("rejected_at", current_timestamp()).withColumn("batch_id", lit(batch_id)).withColumn("source_file", lit("olist_order_payments_dataset"))
            

good_payments = df_payments_clean.filter(
    (col("order_id").isNotNull()) &
    (col("payment_installments")>=1) &
    (col("payment_sequential")>=1) &
    (col("payment_type")!="not_defined") &
    (col("payment_value")>0)
    ).withColumn("ingested_at", current_timestamp()).withColumn("batch_id", lit(batch_id)).withColumn("source_file", lit("olist_order_payments_dataset"))
total = df_payments_raw.count()
good = good_payments.count()
bad = bad_payments.count()
print(f"Total: {total} | Good: {good} | Bad: {bad} | Good+Bad: {good+bad}")

     
            

# COMMAND ----------

bad_payments.groupBy("rejection_reason").count().show(truncate=False)

# COMMAND ----------

good_payments.write.format("delta").mode("overwrite").save(silver_path + "payments")
bad_payments.write.format("delta").mode("overwrite").save(quarantine_path + "payments")
print(spark.read.format("delta").load(silver_path + "payments").count())
print(spark.read.format("delta").load(quarantine_path + "payments").count())

# COMMAND ----------

#5th table reviews
df_reviews_raw = spark.read.format("delta").load(bronze_path+"olist_order_reviews_dataset")
print("Reviews raw count: ", df_reviews_raw.count())
df_reviews_raw.printSchema()


# COMMAND ----------

df_reviews_raw.filter(col("review_score").isNull()).count()

# COMMAND ----------

df_reviews_raw.filter(col("review_score").isNull()).select("review_comment_title", "review_comment_message", "review_answer_timestamp").show(10, truncate=False)

# COMMAND ----------

df_reviews_raw.filter(col("review_score").isNull()).select("review_creation_date").show(10)

# COMMAND ----------

bad_reviews_records = df_reviews_raw.filter(
    (col("order_id").isNull()) |
    (col("review_id").isNull()) |
    (((col("review_score").isNull()) | ((col("review_score")<1) | (col("review_score")>5))))
).withColumn("rejection_reason", 
             when(col("order_id").isNull(), lit("null_order_id"))
             .when(col("review_id").isNull(), lit("null_review_id"))
             .when((col("review_score").isNull()) | 
                   ((col("review_score")<1) | (col("review_score")>5))
                   ,lit("missing_review_data")).otherwise("unknown_reason")
             ).withColumn("rejected_at", current_timestamp()).withColumn("batch_id", lit(batch_id)).withColumn("source_file", lit("olist_order_reviews_dataset"))

good_reviews_records = df_reviews_raw.filter(
    (col("order_id").isNotNull()) &
    (col("review_id").isNotNull()) &
    (col("review_score").isNotNull()) &
    (col("review_score") >= 1) &
    (col("review_score") <= 5)
).withColumn("ingested_at", current_timestamp()) \
 .withColumn("batch_id", lit(batch_id)) \
 .withColumn("source_file", lit("olist_order_reviews_dataset"))

total = df_reviews_raw.count()
good = good_reviews_records.count()
bad = bad_reviews_records.count()
print(f"Total: {total} | Good: {good} | Bad: {bad} | Good+Bad: {good+bad}")

# COMMAND ----------

bad_reviews_records.filter(col("rejection_reason") == "missing_review_data") \
    .filter(col("review_score").isNotNull()) \
    .select("review_id", "order_id", "review_score").show(truncate=False) #malformed csv cause bad row where review id got review title and order id got review comment

# COMMAND ----------

good_reviews_records.write.format("delta").mode("overwrite").save(silver_path + "reviews")
bad_reviews_records.write.format("delta").mode("overwrite").save(quarantine_path + "reviews")
print(spark.read.format("delta").load(silver_path + "reviews").count())
print(spark.read.format("delta").load(quarantine_path + "reviews").count())

# COMMAND ----------

#6th table products
df_products_raw = spark.read.format("delta").load(bronze_path + "olist_products_dataset")
print(f"Products raw count: {df_products_raw.count()}")
df_products_raw.printSchema()

# COMMAND ----------

#dropping 3 unnecessary cols - product_name_lenght, product_description_lenght, product_photos_qty
df_products_clean = df_products_raw.drop("product_name_lenght", "product_description_lenght", "product_photos_qty")

# COMMAND ----------

df_products_clean.select("product_category_name").distinct().count()

# COMMAND ----------

#applying text standardization bcz checking for valid category (74 values which could be evolving) makes no sense
#checking for these many values makes no sense
df_products_clean = df_products_clean.withColumn("product_category_name", trim(lower(col("product_category_name")))) 


# COMMAND ----------

df_products_clean.printSchema()

# COMMAND ----------

df_products_clean.filter(col("product_category_name").isNull()).count()

# COMMAND ----------

df_products_clean.filter(col("product_weight_g").isNull()).count()

# COMMAND ----------

df_products_clean.filter(col("product_weight_g") < 0).count()

# COMMAND ----------

bad_products = df_products_clean.filter(

    (col("product_id").isNull()) |
    (col("product_category_name").isNull()) |
    ((col("product_weight_g").isNull()) | (col("product_weight_g")<0))
).withColumn("rejection_reason", 
             when((col("product_id").isNull()), lit("missing_product_id"))
             .when((col("product_category_name").isNull()), lit("missing_product_category"))
             .when(((col("product_weight_g").isNull()) | (col("product_weight_g")<0)), lit("invalid_product_weight"))
             .otherwise(lit("unknown"))
             
             ).withColumn("rejected_at", current_timestamp()).withColumn("batch_id", lit(batch_id)).withColumn("source_file", lit("olist_products_dataset"))

good_products= df_products_clean.filter(
    (col("product_id").isNotNull()) &
    (col("product_category_name").isNotNull()) &
    ((col("product_weight_g").isNotNull()) & (col("product_weight_g")>=0))
).withColumn("ingested_at", current_timestamp()).withColumn("batch_id", lit(batch_id)).withColumn("source_file", lit("olist_products_dataset"))

total = df_products_clean.count()
good = good_products.count()
bad = bad_products.count()
print(f"Total: {total} | Good: {good} | Bad: {bad} | Good+Bad: {good+bad}")


# COMMAND ----------

good_products.write.format("delta").mode("overwrite").save(silver_path + "products")
bad_products.write.format("delta").mode("overwrite").save(quarantine_path + "products")
print(spark.read.format("delta").load(silver_path + "products").count())
print(spark.read.format("delta").load(quarantine_path + "products").count())

# COMMAND ----------

#7th table sellers

df_sellers_raw = spark.read.format("delta").load(bronze_path + "olist_sellers_dataset")
print(f"Sellers raw count: {df_sellers_raw.count()}")
df_sellers_raw.printSchema()

# COMMAND ----------

df_sellers_clean = df_sellers_raw.withColumn("seller_city", trim(lower(col("seller_city")))) \
    .withColumn("seller_state", trim(lower(col("seller_state"))))

# COMMAND ----------

bad_sellers_records = df_sellers_clean.filter(
    (col("seller_id").isNull()) |
    (length(col("seller_zip_code_prefix"))!=5)
).withColumn("rejection_reason", 
             when(col("seller_id").isNull(), lit("missing_seller_id"))
             .when(length(col("seller_zip_code_prefix"))!=5, lit("invalid_seller_zip_code"))
             .otherwise(lit("unknown_reason"))
             ).withColumn("rejected_at", current_timestamp()).withColumn("batch_id", lit(batch_id)).withColumn("source_file",lit("olist_sellers_dataset"))

good_sellers_records = df_sellers_clean.filter(
    (col("seller_id").isNotNull()) &
    (length(col("seller_zip_code_prefix"))==5)
).withColumn("ingested_at", current_timestamp()).withColumn("batch_id", lit(batch_id)).withColumn("source_file",lit("olist_sellers_dataset"))
total = df_sellers_clean.count()
good = good_sellers_records.count()
bad = bad_sellers_records.count()
print(f"Total: {total} | Good: {good} | Bad: {bad} | Good+Bad: {good+bad}")


# COMMAND ----------

df_sellers_clean.select("seller_state").distinct().orderBy("seller_state").count()

# COMMAND ----------

good_sellers_records.write.format("delta").mode("overwrite").save(silver_path + "sellers")
bad_sellers_records.write.format("delta").mode("overwrite").save(quarantine_path + "sellers")
print(spark.read.format("delta").load(silver_path + "sellers").count())
print(spark.read.format("delta").load(quarantine_path + "sellers").count())

# COMMAND ----------

#8 table translation
df_translation_raw = spark.read.format("delta").load(bronze_path + "product_category_name_translation")
print(f"Translation raw count: {df_translation_raw.count()}")
df_translation_raw.printSchema()

# COMMAND ----------

df_translation_raw.filter(col("product_category_name").isNull() | col("product_category_name_english").isNull()).count()

# COMMAND ----------

df_translation_clean = df_translation_raw.withColumn("product_category_name", trim(lower(col("product_category_name")))) \
    .withColumn("product_category_name_english", trim(lower(col("product_category_name_english")))) \
    .withColumn("ingested_at", current_timestamp()) \
    .withColumn("batch_id", lit(batch_id)) \
    .withColumn("source_file", lit("product_category_name_translation"))


# COMMAND ----------

df_translation_clean.write.format("delta").mode("overwrite").save(silver_path + "product_category_translation")
print(spark.read.format("delta").load(silver_path + "product_category_translation").count())

# COMMAND ----------

# MAGIC %md
# MAGIC FK integrity check

# COMMAND ----------

orders_silver = spark.read.format("delta").load(silver_path + "orders")
order_items_silver = spark.read.format("delta").load(silver_path + "order_items")

# COMMAND ----------

orphaned_order_items = order_items_silver.join(orders_silver, on="order_id", how="left_anti")
print(f"Orphaned order_items (order_id not in orders): {orphaned_order_items.count()}")

#order_item is child
#orders id parent 
#we are checking for orphaned_rows - rows that are in child but not in parent!

# COMMAND ----------

products_silver = spark.read.format("delta").load(silver_path + "products")
sellers_silver = spark.read.format("delta").load(silver_path + "sellers")

orphaned_products_fk = order_items_silver.join(products_silver, on="product_id", how="left_anti")
print(f"Orphaned order_items (product_id not in products): {orphaned_products_fk.count()}")

orphaned_sellers_fk = order_items_silver.join(sellers_silver, on="seller_id", how="left_anti")
print(f"Orphaned order_items (seller_id not in sellers): {orphaned_sellers_fk.count()}")

# COMMAND ----------

products_quarantine = spark.read.format("delta").load(quarantine_path + "products")
orphaned_products_fk.join(products_quarantine, on="product_id", how="inner").count()

# COMMAND ----------

orphaned_products_fk.select("product_id").distinct().count()

# COMMAND ----------

order_items_silver_clean = order_items_silver.join(orphaned_products_fk, on="product_id", how="left_anti")
print(f"Order items after removing orphans: {order_items_silver_clean.count()}")

# COMMAND ----------

#overwrite silver  order_items (orphanes rowws removed) and append orphaned rows
order_items_silver_clean.write.format("delta").mode("overwrite").save(silver_path + "order_items")


# COMMAND ----------

print(spark.read.format("delta").load(quarantine_path + "order_items").columns)


# COMMAND ----------

print(spark.read.format("delta").load(silver_path + "order_items").count())


# COMMAND ----------

print(spark.read.format("delta").load(quarantine_path + "order_items").count())

# COMMAND ----------

#print("orphaned_products_fk:", orphaned_products_fk.count())

#print("orphaned_products_fk_tagged_final:", orphaned_products_fk_tagged_final.count())

# COMMAND ----------

orphaned_products_fk_v2 = good_order_items.join(products_silver, on="product_id", how="left_anti")
print(f"Orphaned order_items (recovered): {orphaned_products_fk_v2.count()}")

# COMMAND ----------

orphaned_products_fk_tagged_v2 = orphaned_products_fk_v2.withColumn("rejection_reason", lit("parent_product_quarantined")) \
    .withColumn("rejected_at", current_timestamp()) \
    .withColumn("batch_id", lit(batch_id)) \
    .withColumn("source_file", lit("olist_order_items_dataset"))

# COMMAND ----------

print(orphaned_products_fk_tagged_v2.columns)

# COMMAND ----------

orphaned_products_fk_tagged_v2_final = orphaned_products_fk_tagged_v2.select(
    "order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date",
    "price", "freight_value", "rejection_reason", "rejected_at", "batch_id", "source_file"
)
print(orphaned_products_fk_tagged_v2_final.count())

# COMMAND ----------

orphaned_products_fk_tagged_v2_final.write.format("delta").mode("append").save(quarantine_path + "order_items")
print(spark.read.format("delta").load(quarantine_path + "order_items").count())

# COMMAND ----------

print(spark.read.format("delta").load(silver_path + "order_items").count()) #d

# COMMAND ----------

#FK Check for reviews and payments table 
reviews_silver = spark.read.format("delta").load(silver_path + "reviews")
payments_silver = spark.read.format("delta").load(silver_path + "payments")

orphaned_reviews_fk = reviews_silver.join(orders_silver, on="order_id", how="left_anti")
print(f"Orphaned reviews (order_id not in orders): {orphaned_reviews_fk.count()}")

orphaned_payments_fk = payments_silver.join(orders_silver, on="order_id", how="left_anti")
print(f"Orphaned payments (order_id not in orders): {orphaned_payments_fk.count()}")

# COMMAND ----------

