Why batch ingestion? 

The Olist dataset is a static historical export, it does not update in real time. The business KPIs this pipeline computes (repeat customer rate, delivery delays, seller performance scores) are analytical metrics that only require daily or weekly refreshes, not real-time updates. Batch ingestion via Airflow is therefore the appropriate choice, it keeps the pipeline simple, reliable, and easy to debug. 
Streaming would add unnecessary complexity (event brokers, checkpointing, watermarking) without any business benefit for this use case.


Why Delta Lake? 

Delta Lake is the storage format used across all three layers of this pipeline. 
While Delta Lake stores data in Parquet format underneath, it adds critical production-grade features that plain Parquet lacks: ACID transactions ensure no partial or corrupt writes, a transaction log (_delta_log) provides a full audit trail of every operation, time travel allows querying or rolling back to previous versions of any table, and schema enforcement prevents malformed data from silently corrupting downstream layers. 
These features make Delta Lake the industry standard for medallion architecture pipelines and the natural choice for a portfolio project targeting production-grade engineering practices.


Why does quarantine branch off Silver?

The quarantine layer captures records that fail data quality checks, null primary keys, negative prices, invalid foreign key references, out-of-range values and routes them to a separate Delta table with a rejection_reason column instead of silently dropping them. 
Silver is the correct layer for this because it is the first point where business rules are applied and "bad" can be meaningfully defined. 
Bronze preserves raw data without judgment. Gold consumes only clean, validated data. Quarantining at Silver acts as a quality gate between raw ingestion and analytical consumption ensuring Gold layer KPIs are never silently corrupted by bad records, while also maintaining a full audit trail of every rejected row and the reason for its rejection.


Why is Gold denormalized as a star schema?

The Silver layer preserves a normalized structure close to the source data, which is appropriate for data quality and transformation work. The Gold layer deliberately denormalizes into a star schema because its consumers analysts and BI tools need fast, simple, consistent access to data. 
Pre-joining all relevant tables into fact_order_items at load time eliminates repeated join logic in every downstream query, reduces query execution time, and ensures consistent metric definitions across all reports. Derived columns like delivery_delay_days and is_late_delivery are computed once at Gold layer rather than recalculated differently by each consumer. The star schema structure fact table at center, dimension tables at the edges provides the right balance between denormalization for speed and structure for clarity.


