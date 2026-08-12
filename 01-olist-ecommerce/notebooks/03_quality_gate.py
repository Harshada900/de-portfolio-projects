# Databricks notebook source
from pyspark.sql.functions import col, lit

silver_path = "/Volumes/workspace/default/olist_raw_data/silver_v2/"
quarantine_path = "/Volumes/workspace/default/olist_raw_data/quarantine_v2/"
gold_path = "/Volumes/workspace/default/olist_raw_data/gold/"


# COMMAND ----------

#Data Quality Check
tables = ["orders", "customers", "order_items", "payments", "reviews", "products", "sellers"]
quality_summary = []

for table in tables:
    good_count = spark.read.format("delta").load(silver_path+table).count()
    bad_count = spark.read.format("delta").load(quarantine_path+table).count()
    total = good_count+bad_count
    rejection_rate = round((bad_count/total)*100, 2)
    quality_summary.append((table, total, good_count, bad_count, rejection_rate))

for row in quality_summary:
    print(row)

# COMMAND ----------

# since this is list of tuples we'll convert it to df

from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

quality_schema = StructType([
    StructField("table_name", StringType(), True),
    StructField("total_rows", LongType(), True),
    StructField("good_rows", LongType(), True),
    StructField("bad_rows", LongType(), True),
    StructField("rejection_rate", DoubleType(), True)
])

quality_summary_df = spark.createDataFrame(quality_summary, schema=quality_schema)
quality_summary_df.show()

# COMMAND ----------

reason_breakdown = None

for table in tables:
    bad_df = spark.read.format("delta").load(quarantine_path+table)\
        .groupBy("rejection_reason").count()\
            .withColumn("table_name", lit(table))
    reason_breakdown = bad_df if reason_breakdown is None else reason_breakdown.union(bad_df)


reason_breakdown.select("table_name", "rejection_reason", "count").orderBy("table_name", col("count").desc()).show(truncate=False)

# COMMAND ----------

quality_summary_df.write.format("delta").mode("overwrite").save(gold_path + "data_quality_summary")
reason_breakdown.select("table_name", "rejection_reason", "count").write.format("delta").mode("overwrite").save(gold_path + "data_quality_reason_breakdown")

print(spark.read.format("delta").load(gold_path + "data_quality_summary").count())
print(spark.read.format("delta").load(gold_path + "data_quality_reason_breakdown").count())

# COMMAND ----------

THRESHOLD = 5.0
failed_tables = quality_summary_df.filter(col("rejection_rate") > THRESHOLD)

if failed_tables.count() > 0:
    failed_list = [row["table_name"] for row in failed_tables.collect()]
    raise Exception(f"DATA QUALITY GATE FAILED: the following tables exceed {THRESHOLD}% rejection rate: {failed_list}")
else:
    print(f"Data quality gate PASSED | all tables under {THRESHOLD}% rejection rate.")