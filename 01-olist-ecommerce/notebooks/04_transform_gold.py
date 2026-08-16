# Databricks notebook source
from pyspark.sql.functions import (
    col, lit, when, trim, lower, current_timestamp, sum as spark_sum, min as spark_min, max as spark_max,
    monotonically_increasing_id, row_number, desc, asc
)
from pyspark.sql.window import Window
import uuid

silver_path = "/Volumes/workspace/default/olist_raw_data/silver_v2/"
gold_path = "/Volumes/workspace/default/olist_raw_data/gold/"
quarantine_path = "/Volumes/workspace/default/olist_raw_data/quarantine_v2/"
bronze_path = "/Volumes/workspace/default/olist_raw_data/bronze/"

batch_id = str(uuid.uuid4())
print(f"Gold pipeline batch ID: {batch_id}")

# COMMAND ----------

#read silver cleaned data
orders_silver = spark.read.format("delta").load(silver_path + "orders")
order_items_silver = spark.read.format("delta").load(silver_path + "order_items")
payments_silver = spark.read.format("delta").load(silver_path + "payments")
reviews_silver = spark.read.format("delta").load(silver_path + "reviews")
products_silver = spark.read.format("delta").load(silver_path + "products")
sellers_silver = spark.read.format("delta").load(silver_path + "sellers")
customers_silver = spark.read.format("delta").load(silver_path + "customers")
translation_silver = spark.read.format("delta").load(silver_path + "product_category_translation")

# COMMAND ----------

print("orders:", orders_silver.count())
print("order_items:", order_items_silver.count())
print("payments:", payments_silver.count())
print("reviews:", reviews_silver.count())
print("products:", products_silver.count())
print("sellers:", sellers_silver.count())
print("customers:", customers_silver.count())
print("translation:", translation_silver.count())

# COMMAND ----------

payments_agg = payments_silver.groupBy("order_id").agg(spark_sum("payment_value").alias("total_payment_value"))
print(f"Payments aggregated rows: {payments_agg.count()}")
# got 99435 instead of  99441 - means 6 orders dont have any payment record at all
# 6 orders have no payment records in payments silver

# COMMAND ----------

reviews_silver.groupBy("order_id").count().filter(col("count")>1).count()
#this means 547 rows has more than 1 review
# 547 is just telling us that there are more than 1 reviews for the same id

# COMMAND ----------

reviews_silver.printSchema()

# COMMAND ----------

reviews_silver.groupBy("order_id").count().filter(col("count") > 1).show(5,truncate=False)

# COMMAND ----------

reviews_silver.filter(col("order_id") == "f63a31c3349b87273468ff7e66852056") \
    .select("review_id", "review_score", "review_creation_date", "review_answer_timestamp") \
    .show(truncate=False) 

# COMMAND ----------

reviews_silver.groupBy("order_id").agg(
    spark_min("review_score").alias("min_score"),
    spark_max("review_score").alias("max_score")
).filter(col("min_score") != col("max_score")).count()

# COMMAND ----------

review_window = Window.partitionBy("order_id").orderBy(desc("review_answer_timestamp"))

reviews_deduped = reviews_silver.withColumn("review_rank", row_number().over(review_window)) \
    .filter(col("review_rank") == 1) \
    .drop("review_rank")

print(f"Reviews after dedup: {reviews_deduped.count()}")

# COMMAND ----------

reviews_deduped.groupBy("order_id").count().filter(col("count") > 1).count()

# COMMAND ----------

dim_customer = customers_silver.select(
    "customer_id","customer_unique_id", "customer_city", "customer_state", "customer_zip_code_prefix"
).withColumn("customer_sk", monotonically_increasing_id())
print(f"dimenstion customer rows: {dim_customer.count()}")

# COMMAND ----------

dim_seller = sellers_silver.select(
    "seller_id", "seller_city", "seller_state", "seller_zip_code_prefix"
).withColumn("seller_sk", monotonically_increasing_id())

print(f"dimenstion seller rows: {dim_seller.count()}")

# COMMAND ----------

dim_product = products_silver.join(translation_silver, on="product_category_name", how="left").select(
    "product_id", "product_category_name", "product_category_name_english",
    "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"
).withColumn("product_sk", monotonically_increasing_id())

print(f"dimension product rows: {dim_product.count()}")

# COMMAND ----------

#products_silver.select("product_category_name").distinct().count()


# COMMAND ----------

#translation_silver.select("product_category_name").distinct().count()

# COMMAND ----------

#step 1 — figure out the date range that needs to cover.
orders_silver.select(spark_min("order_purchase_timestamp"), spark_max("order_purchase_timestamp")).show()

# COMMAND ----------

orders_silver.select(spark_min("order_delivered_customer_date"), spark_max("order_delivered_customer_date")).show()
orders_silver.select(spark_min("order_estimated_delivery_date"), spark_max("order_estimated_delivery_date")).show()

# COMMAND ----------

from pyspark.sql.functions import sequence, to_date, explode, year, month, quarter, dayofweek, date_format, datediff
date_df = spark.sql("select explode(sequence(to_date('2016-01-01'),to_date('2018-12-31') ,interval 1 day )) as full_date")
print(f"Total dates generated: {date_df.count()}")
date_df.show(5)


# COMMAND ----------

dim_date = date_df.withColumn("date_sk", monotonically_increasing_id()).withColumn("year", year("full_date"))\
    .withColumn("month", month("full_date"))\
    .withColumn("month_name", date_format("full_date", "MMMM"))\
    .withColumn("quarter", quarter("full_date"))\
    .withColumn("day_of_week", dayofweek("full_date"))\
    .withColumn("day_name", date_format("full_date", "EEEE"))\
    .withColumn("is_weekend", when(dayofweek("full_date").isin(1,7), True).otherwise(False))


dim_date.show(5)

print(f"Dimension date rows: {dim_date.count()}")

# COMMAND ----------

dim_customer.write.format("delta").mode("overwrite").save(gold_path + "dim_customer")
dim_seller.write.format("delta").mode("overwrite").save(gold_path + "dim_seller")
dim_product.write.format("delta").mode("overwrite").save(gold_path + "dim_product")
dim_date.write.format("delta").mode("overwrite").save(gold_path + "dim_date")

# COMMAND ----------

dim_seller.columns

# COMMAND ----------

print(spark.read.format("delta").load(gold_path + "dim_customer").count())
print(spark.read.format("delta").load(gold_path + "dim_seller").count())
print(spark.read.format("delta").load(gold_path + "dim_product").count())
print(spark.read.format("delta").load(gold_path + "dim_date").count())

# COMMAND ----------

fact_base = order_items_silver.join(orders_silver, on="order_id", how="left")
print(f"fact_base rows: {fact_base.count()}")

# COMMAND ----------

fact_base2 = fact_base.join(payments_agg, on="order_id", how="left").join(reviews_deduped.select("order_id","review_score"), on="order_id", how="left")

print(f"fact_base2 rows: {fact_base2.count()}")


# COMMAND ----------

fact_base2.printSchema()

# COMMAND ----------

#replace natural keys with surrogate keys
fact_base3 = fact_base2.join(
    dim_customer.select("customer_id", "customer_sk"), on="customer_id", how="left"
)
print(f"fact_base3 rows: {fact_base3.count()}")

# COMMAND ----------

fact_base3.printSchema()

# COMMAND ----------

fact_base4 = fact_base3.join(
    dim_seller.select("seller_id", "seller_sk"), on="seller_id", how="left"
)
print(f"fact_base4 rows: {fact_base4.count()}")

# COMMAND ----------

fact_base5 = fact_base4.join(
    dim_product.select("product_id", "product_sk"), on="product_id", how="left"
)
print(f"fact_base5 rows: {fact_base5.count()}")

# COMMAND ----------

fact_base5.printSchema()

# COMMAND ----------

fact_clean = fact_base5.select(
    "order_id", "order_item_id", "customer_sk", "seller_sk", "product_sk",
    "order_purchase_timestamp", "price", "freight_value", "total_payment_value",
    "review_score", "order_delivered_customer_date", "order_estimated_delivery_date"
)
print(fact_clean.columns)
print(f"fact_clean rows: {fact_clean.count()}")

# COMMAND ----------

fact_clean2 = fact_clean.withColumn("order_purchase_date", to_date("order_purchase_timestamp"))
fact_clean3 = fact_clean2.join(dim_date.select("full_date","date_sk"), fact_clean2["order_purchase_date"] == dim_date["full_date"], how ="left")
print(f"fact_clean3 rows: {fact_clean3.count()}")

# COMMAND ----------

dim_date.printSchema()

# COMMAND ----------

fact_clean3.filter(col("date_sk").isNull()).count()

# COMMAND ----------

fact_clean4 = fact_clean3.withColumn("delivery_delay_days", datediff(col("order_delivered_customer_date"),col("order_estimated_delivery_date")))

# COMMAND ----------

fact_clean4.filter(col("order_delivered_customer_date").isNull()).select("delivery_delay_days").show(5)

# COMMAND ----------

fact_clean5 = fact_clean4.withColumn(
    "is_late_delivery",
    when(col("delivery_delay_days").isNull(), lit(None)) \
        .when(col("delivery_delay_days") > 0, lit(True)) \
        .otherwise(lit(False))
)

# if deliverty not arrived yet so -> NULL cant be false

# COMMAND ----------

fact_clean5.groupBy("is_late_delivery").count().show()

# COMMAND ----------

fact_clean5.printSchema()

# COMMAND ----------

fact_order_items = fact_clean5.withColumnRenamed("total_payment_value", "payment_value") \
    .withColumnRenamed("date_sk", "order_date_sk") \
    .drop("order_purchase_timestamp", "order_purchase_date", "full_date", 
          "order_delivered_customer_date", "order_estimated_delivery_date") \
    .withColumn("order_item_sk", monotonically_increasing_id())

print(fact_order_items.columns)
print(f"fact_order_items rows: {fact_order_items.count()}")

# COMMAND ----------

fact_order_items.write.format("delta").mode("overwrite").save(gold_path + "fact_order_items")
print(spark.read.format("delta").load(gold_path + "fact_order_items").count())

# COMMAND ----------

#simulating SCD type 2 for seller
dim_seller.show(5, truncate=False)

# COMMAND ----------

seller_first_order = order_items_silver.join(orders_silver, on="order_id", how="left").groupBy("seller_id").agg(spark_min("order_purchase_timestamp").alias("seller_valid_from"))

seller_first_order.orderBy("seller_valid_from").show(5)

# COMMAND ----------

dim_seller_scd = dim_seller.join(seller_first_order, on="seller_id", how="left")\
    .withColumn("valid_from", to_date(col("seller_valid_from")))\
        .withColumn("valid_to", lit(None).cast("date"))\
            .withColumn("is_current", lit(True))\
                .drop("seller_valid_from")

dim_seller_scd.show(5)

# COMMAND ----------

print(f"dim_seller_scd rows: {dim_seller_scd.count()}")
dim_seller_scd.filter(col("valid_from").isNull()).count()

# COMMAND ----------

order_items_silver.select("seller_id").distinct().count() # means 60 sellers present in dim_seller/sellers but they never made sale so 60 nulls

# COMMAND ----------

old_seller_row = dim_seller_scd.filter((col("seller_id")=="3442f8959a84dea7ee197c632cb2df15"))\
    .withColumn("valid_to", to_date(lit("2018-06-01")))\
        .withColumn("is_current", lit(False))

old_seller_row.show()

# COMMAND ----------

new_seller_row = old_seller_row.withColumn("seller_city", lit("rio de janeiro")) \
    .withColumn("seller_state", lit("rj")) \
    .withColumn("seller_sk", lit(9999)) \
    .withColumn("valid_from", to_date(lit("2018-06-01"))) \
    .withColumn("valid_to", lit(None).cast("date")) \
    .withColumn("is_current", lit(True))

new_seller_row.show()

# COMMAND ----------

dim_seller_scd_final = dim_seller_scd.filter(col("seller_id") != "3442f8959a84dea7ee197c632cb2df15") \
    .union(old_seller_row) \
    .union(new_seller_row)

print(f"dim_seller_scd_final rows: {dim_seller_scd_final.count()}")

# COMMAND ----------

dim_seller_scd_final.filter(col("seller_id") == "3442f8959a84dea7ee197c632cb2df15").show()

# COMMAND ----------

# NOTE: This SCD Type 2 change is a simulated/manual example for demonstration purposes,
# since the raw Olist dataset has no real seller attribute history to detect changes from.
dim_seller_scd_final.write.format("delta").mode("overwrite").save(gold_path + "dim_seller_scd")
print(spark.read.format("delta").load(gold_path + "dim_seller_scd").count())

# COMMAND ----------

#investigating null_order_id
reviews_silver_check = spark.read.format("delta").load(bronze_path + "olist_order_reviews_dataset")
reviews_silver_check.filter(col("order_id").isNull()).count()

# COMMAND ----------

reviews_silver_check.filter(col("order_id").isNull() & col("review_score").isNull()).count() #means orde_id and review_score both are nulls 

# COMMAND ----------

# MAGIC %md
# MAGIC **KPIs**

# COMMAND ----------

customer_order_window = Window.partitionBy("customer_unique_id").orderBy("order_purchase_timestamp")

customer_orders_ranked = fact_order_items.join(orders_silver.select("order_id", "order_purchase_timestamp"), on="order_id", how="left")\
    .join(dim_customer.select("customer_sk","customer_unique_id"), on="customer_sk", how="left")\
        .select("customer_unique_id","order_id","order_purchase_timestamp").distinct()\
            .withColumn("order_rank", row_number().over(customer_order_window))


customer_orders_ranked.show(10)


# COMMAND ----------

customer_orders_ranked.groupBy("order_rank").count().orderBy("order_rank").show(10)

# COMMAND ----------

# now finiding the no. of orders per month

customer_orders_ranked.createOrReplaceTempView("customer_orders_ranked")

customer_summary = spark.sql("""
          select
          customer_unique_id,
          min(order_purchase_timestamp) as first_order_date,
          count(order_id) as total_orders,
          date_format(min(order_purchase_timestamp),'yyyy-MM') as cohort_month
          from customer_orders_ranked
          group by customer_unique_id
          """)

customer_summary.show(10)

# COMMAND ----------

customer_summary.createOrReplaceTempView("customer_summary")

cohort_kpi = spark.sql("""
          select
          cohort_month,
          count(customer_unique_id) as total_customers,
          sum(case when total_orders>1 then 1 else 0 end) as repeat_customers,
          round( sum(case when total_orders>1 then 1 else 0 end)*100.0/count(customer_unique_id) ,2) as repeat_rate_pct
          from customer_summary
          group by cohort_month
          order by cohort_month
          """)

cohort_kpi.show(30)

# COMMAND ----------

cohort_kpi.write.format("delta").mode("overwrite").save(gold_path + "kpi_customer_repurchase_cohort")
print(spark.read.format("delta").load(gold_path + "kpi_customer_repurchase_cohort").count())

# COMMAND ----------

#kpi 2 - 
fact_order_items.createOrReplaceTempView("fact_order_items")

seller_delivery_stats = spark.sql("""
          select 
          seller_sk,
          count(*) AS total_orders,
          sum(case when is_late_delivery=true then 1 else 0 end) as late_orders,
          round(sum(case when is_late_delivery=True then 1 else 0 end)*100.0/count(*),2) as late_delivery_rate,
          round(avg(delivery_delay_days),2) as avg_delay_days
          from fact_order_items
          where delivery_delay_days is not null
          group by seller_sk
          """)

seller_delivery_stats.show(10)

# COMMAND ----------

seller_delivery_stats.selectExpr(
    "percentile_approx(avg_delay_days, 0.25) as p25",
    "percentile_approx(avg_delay_days, 0.5) as median",
    "percentile_approx(avg_delay_days, 0.75) as p75",
    "percentile_approx(late_delivery_rate, 0.25) as late_rate_p25",
    "percentile_approx(late_delivery_rate, 0.5) as late_rate_median",
    "percentile_approx(late_delivery_rate, 0.75) as late_rate_p75"
).show()

# COMMAND ----------

seller_delivery_stats.createOrReplaceTempView("seller_delivery_stats")

seller_tiers = spark.sql("""
    SELECT
        seller_sk,
        total_orders,
        late_delivery_rate,
        avg_delay_days,
        CASE
            WHEN late_delivery_rate = 0 THEN 'Elite'
            WHEN late_delivery_rate <= 7.41 THEN 'Good'
            WHEN late_delivery_rate <= 20 THEN 'At Risk'
            ELSE 'Critical'
        END AS delivery_tier
    FROM seller_delivery_stats
""")

seller_tiers.groupBy("delivery_tier").count().show()

# COMMAND ----------

print(1664 + 522 + 493 + 235)
print(seller_delivery_stats.count())

# COMMAND ----------

fact_order_items.createOrReplaceTempView("fact_order_items")
dim_product.createOrReplaceTempView("dim_product")

spark.sql("""
          select seller_sk,
          count(distinct product_category_name) as num_categories
          from fact_order_items f
          left join dim_product p
          on f.product_sk=p.product_sk
          group by seller_sk
          order by num_categories desc
          """).show()


# COMMAND ----------

spark.sql("""
    SELECT num_categories, COUNT(*) as seller_count
    FROM (
        SELECT seller_sk, COUNT(DISTINCT product_category_name) AS num_categories
        FROM fact_order_items f
        JOIN dim_product p ON f.product_sk = p.product_sk
        GROUP BY seller_sk
    )
    GROUP BY num_categories
    ORDER BY num_categories
""").show(20)

# COMMAND ----------

seller_category_stats = spark.sql("""
    SELECT
        f.seller_sk,
        p.product_category_name,
        COUNT(*) AS total_orders,
        SUM(CASE WHEN f.is_late_delivery = true THEN 1 ELSE 0 END) AS late_orders,
        ROUND(SUM(CASE WHEN f.is_late_delivery = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS late_delivery_rate,
        ROUND(AVG(f.delivery_delay_days), 2) AS avg_delay_days
    FROM fact_order_items f
    JOIN dim_product p ON f.product_sk = p.product_sk
    WHERE f.delivery_delay_days IS NOT NULL
    GROUP BY f.seller_sk, p.product_category_name
""")

seller_category_stats.show(10)
print(f"seller_category_stats rows: {seller_category_stats.count()}")

# COMMAND ----------

seller_category_stats.createOrReplaceTempView("seller_category_stats")

seller_category_ranked = spark.sql("""
          select 
          seller_sk,
          product_category_name,
          total_orders,
          late_orders,
          late_delivery_rate,
          avg_delay_days,
          rank() over(partition by product_category_name order by late_delivery_rate asc) as category_rank
          from seller_category_stats
          """)

seller_category_ranked.show(10)

# COMMAND ----------

seller_category_ranked.createOrReplaceTempView("seller_category_ranked")

seller_category_pct = spark.sql("""
    SELECT
        seller_sk,
        product_category_name,
        late_delivery_rate,
        avg_delay_days,
        category_rank,
        COUNT(*) OVER (PARTITION BY product_category_name) AS total_sellers_in_category,
        ROUND(category_rank * 100.0 / COUNT(*) OVER (PARTITION BY product_category_name), 2) AS rank_percentile
    FROM seller_category_ranked
""")

seller_category_pct.show()

# COMMAND ----------

seller_category_pct.createOrReplaceTempView("seller_category_pct")

seller_final_tiers = spark.sql("""
    SELECT
        seller_sk,
        product_category_name,
        late_delivery_rate,
        avg_delay_days,
        rank_percentile,
        CASE
            WHEN rank_percentile <= 25 THEN 'Elite'
            WHEN rank_percentile <= 50 THEN 'Good'
            WHEN rank_percentile <= 75 THEN 'At Risk'
            ELSE 'Critical'
        END AS delivery_tier
    FROM seller_category_pct
""")

seller_final_tiers.groupBy("delivery_tier").count().show()

# COMMAND ----------

seller_final_tiers.write.format("delta").mode("overwrite").save(gold_path + "kpi_seller_delivery_tier")
print(spark.read.format("delta").load(gold_path + "kpi_seller_delivery_tier").count())

# COMMAND ----------

# MAGIC %md
# MAGIC **KPI 3 - Review Score Decay After Late Delivery**

# COMMAND ----------

dim_date.select("full_date").show(10)

# COMMAND ----------

dim_date.show(10)

# COMMAND ----------

fact_order_items.show(10)

# COMMAND ----------

#grain - 1 row per calender day

fact_order_items.createOrReplaceTempView("fact_order_items")
dim_date.createOrReplaceTempView("dim_date")

daily_review_stats = spark.sql("""
          select 
          d.full_date,
          round(avg(f.review_score), 2) as avg_review_score,
          round(avg(case when  f.is_late_delivery=true then 1.0 else 0.0 end)*100, 2) as late_rate_pct,
          count(*) as total_orders
          from fact_order_items f
          join dim_date d 
          on f.order_date_sk=d.date_Sk
          where f.review_score is not null
          group by d.full_date
          order by d.full_date 
          """)

daily_review_stats.show(10)
print(f"daily_review_stats rows {daily_review_stats.count()}")

# COMMAND ----------

orders_silver.select(to_date("order_purchase_timestamp").alias("order_date")).distinct().count() # total rows with at least 1 order - 634 days has at least 1 order out of 773 days

# COMMAND ----------

# days with orders but zero valid reviews
order_days = orders_silver.select(to_date("order_purchase_timestamp").alias("d")).distinct()
review_days = daily_review_stats.select(col("full_date").alias("d")).distinct()
order_days.subtract(review_days).count()

# COMMAND ----------

daily_review_stats.createOrReplaceTempView("daily_review_stats")

review_score_rolling = spark.sql("""
    SELECT
        full_date,
        avg_review_score,
        late_rate_pct,
        total_orders,
        ROUND(AVG(avg_review_score) OVER (
            ORDER BY full_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ), 2) AS rolling_30day_avg_review_score
    FROM daily_review_stats
""")

review_score_rolling.show(35)

# COMMAND ----------

review_score_rolling.write.format("delta").mode("overwrite").save(gold_path + "kpi_review_score_decay")
print(spark.read.format("delta").load(gold_path + "kpi_review_score_decay").count())

# COMMAND ----------

# MAGIC %md
# MAGIC **KPI 4 - 	Revenue Concentration Risk**

# COMMAND ----------

fact_order_items.createOrReplaceTempView("fact_order_items")

seller_revenue = spark.sql("""
    SELECT
        seller_sk,
        ROUND(SUM(price), 2) AS total_revenue
    FROM fact_order_items
    GROUP BY seller_sk
""")

seller_revenue.show(10)
print(f"seller_revenue rows: {seller_revenue.count()}")

# COMMAND ----------

from pyspark.sql.functions import ntile

revenue_window = Window.orderBy(col("total_revenue").desc())

seller_deciles = seller_revenue.withColumn("revenue_decile", ntile(10).over(revenue_window))

seller_deciles.show(30)


# COMMAND ----------

seller_deciles.createOrReplaceTempView("seller_deciles")

revenue_concentration = spark.sql("""
    SELECT
        revenue_decile,
        COUNT(*) AS num_sellers,
        ROUND(SUM(total_revenue), 2) AS decile_revenue,
        ROUND(SUM(total_revenue) * 100.0 / SUM(SUM(total_revenue)) OVER (), 2) AS pct_of_total_revenue
    FROM seller_deciles
    GROUP BY revenue_decile
    ORDER BY revenue_decile
""")

revenue_concentration.show(10)

# COMMAND ----------

revenue_concentration.write.format("delta").mode("overwrite").save(gold_path + "kpi_revenue_concentration")
print(spark.read.format("delta").load(gold_path + "kpi_revenue_concentration").count())

# COMMAND ----------

# MAGIC %md
# MAGIC **KPI 5 - Delivery Gap by Seller-State Corridor**

# COMMAND ----------

fact_order_items.createOrReplaceTempView("fact_order_items")
dim_customer.createOrReplaceTempView("dim_customers")
dim_seller.createOrReplaceTempView("dim_sellers")

corridor_stats = spark.sql("""
          select 
          s.seller_state,
          c.customer_state,
          count(*) as total_orders,
          round(avg(f.delivery_delay_days),2) as avg_delivery_gap_days
          from fact_order_items f 
          join dim_sellers s on f.seller_sk=s.seller_sk
          join dim_customers c on f.customer_sk=c.customer_sk
          where f.delivery_delay_days is not null
          group by s.seller_state, c.customer_state

          """)

corridor_stats.show(100)

# COMMAND ----------

corridor_stats.count()

# COMMAND ----------

corridor_stats.createOrReplaceTempView("corridor_stats")

flagged_corridors = spark.sql("""
    SELECT
        seller_state,
        customer_state,
        total_orders,
        avg_delivery_gap_days
    FROM corridor_stats
    WHERE avg_delivery_gap_days > 2
    AND total_orders >= 10
    ORDER BY avg_delivery_gap_days DESC
""")

flagged_corridors.show(20)

# COMMAND ----------

spark.sql("""
    SELECT seller_state, customer_state, total_orders, avg_delivery_gap_days
    FROM corridor_stats
    WHERE avg_delivery_gap_days > 0
    ORDER BY avg_delivery_gap_days DESC
    LIMIT 20
""").show()

# COMMAND ----------

corridor_stats.write.format("delta").mode("overwrite").save(gold_path + "kpi_delivery_gap_corridor")
flagged_corridors.write.format("delta").mode("overwrite").save(gold_path + "kpi_delivery_gap_flagged")

print(spark.read.format("delta").load(gold_path + "kpi_delivery_gap_corridor").count())
print(spark.read.format("delta").load(gold_path + "kpi_delivery_gap_flagged").count())


# COMMAND ----------

dim_customer.write.format("delta").mode("overwrite").saveAsTable("workspace.default.dim_customer")
dim_seller.write.format("delta").mode("overwrite").saveAsTable("workspace.default.dim_seller")
dim_product.write.format("delta").mode("overwrite").saveAsTable("workspace.default.dim_product")
dim_date.write.format("delta").mode("overwrite").saveAsTable("workspace.default.dim_date")
fact_order_items.write.format("delta").mode("overwrite").saveAsTable("workspace.default.fact_order_items")

cohort_kpi.write.format("delta").mode("overwrite").saveAsTable("workspace.default.kpi_customer_repurchase_cohort")
seller_final_tiers.write.format("delta").mode("overwrite").saveAsTable("workspace.default.kpi_seller_delivery_tier")
review_score_rolling.write.format("delta").mode("overwrite").saveAsTable("workspace.default.kpi_review_score_decay")
revenue_concentration.write.format("delta").mode("overwrite").saveAsTable("workspace.default.kpi_revenue_concentration")
corridor_stats.write.format("delta").mode("overwrite").saveAsTable("workspace.default.kpi_delivery_gap_corridor")

# COMMAND ----------

spark.sql("SHOW TABLES IN workspace.default").show(50, truncate=False)

# COMMAND ----------

#orders_silver.select("order_id").distinct().count()


# COMMAND ----------

#fact_order_items.select("order_id").distinct().count()

# COMMAND ----------

#quarantine_order_items = spark.read.format("delta").load(quarantine_path + "order_items")
#quarantine_order_items.groupBy("rejection_reason").count().show()

# COMMAND ----------

#missing_orders = orders_silver.select("order_id").subtract(fact_order_items.select("order_id"))
#print(f"Total missing orders: {missing_orders.count()}")

#missing_orders.join(quarantine_order_items, on="order_id", how="inner").groupBy("rejection_reason").count().show()

# COMMAND ----------

#missing_orders_list = missing_orders.select("order_id")
#orders_with_zero_items = missing_orders_list.join(
#    order_items_silver.select("order_id").union(quarantine_order_items.select("order_id")).distinct(),
#    on="order_id", how="left_anti"
#)
#print(f"Orders with truly zero order_items anywhere: {orders_with_zero_items.count()}")

# COMMAND ----------

#missing_orders_detail = orders_silver.select("order_id") \
#    .subtract(fact_order_items.select("order_id")) \
#    .join(quarantine_order_items.select("order_id", "rejection_reason"), on="order_id", how="left")

#missing_orders_detail.groupBy("rejection_reason").count().orderBy(col("count").desc()).show()

# COMMAND ----------

#print(missing_orders_detail.count())
#print(missing_orders_detail.select("order_id").distinct().count())

# COMMAND ----------

"""missing_orders_detail_dedup = orders_silver.select("order_id") \
    .subtract(fact_order_items.select("order_id")) \
    .join(quarantine_order_items.select("order_id", "rejection_reason").distinct(), on="order_id", how="left") \
    .dropDuplicates(["order_id"])

missing_orders_detail_dedup.groupBy("rejection_reason").count().orderBy(col("count").desc()).show()
print(missing_orders_detail_dedup.count())"""

# COMMAND ----------

order_count_distribution = spark.sql("""
    SELECT order_rank, COUNT(*) as customer_count
    FROM customer_orders_ranked
    GROUP BY order_rank
    ORDER BY order_rank
""")

order_count_distribution.show(20)

# COMMAND ----------

order_count_distribution.write.format("delta").mode("overwrite").saveAsTable("workspace.default.kpi_order_count_distribution")
print(spark.read.table("workspace.default.kpi_order_count_distribution").count())

# COMMAND ----------

customer_orders_ranked.count()

# COMMAND ----------

