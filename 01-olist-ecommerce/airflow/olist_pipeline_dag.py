from airflow.decorators import dag 
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime, timedelta
import requests
from airflow.models import Variable


def slack_failure_alert(context):
    webhook_url=Variable.get("slack_webhook_url")
    dag_id= context["task_instance"].dag_id
    task_id= context["task_instance"].task_id
    logical_date = context["logical_date"]

    message = {
        "text": f":red_circle: *Task Failed*\n*DAG*:{dag_id}\n*Task*:{task_id}\n*Time:*{logical_date} "
    }
    requests.post(webhook_url, json=message)

default_args = {

    "retries":3,
    "retry_delay":timedelta(minutes=2),
    "on_failure_callback":slack_failure_alert
}
@dag(
    dag_id="olist_pipeline",
    default_args=default_args,
    schedule="0 9 * * *", #    0 minutes, 9 hour, every day, every month, every weekday -> daily at 9 AM
    start_date=datetime(2026,8,1),
    catchup=False,
    doc_md="""
    ### Olist E-commerce Data Pipeline
    Orchestrates the Bronze → Silver → Quality Gate → Gold pipeline for Olist e-commerce data,
    triggering Databricks Jobs and halting on data quality failures.

    - **run_bronze**: Ingests raw CSVs into Bronze Delta tables
    - **run_silver**: Applies schema validation, business rules, and quarantine logic
    - **run_data_quality_gate**: Halts the pipeline if any table's rejection rate exceeds 5%
    - **run_gold**: Builds the star schema (dimensions, fact table, SCD Type 2, KPIs)
    """
)
def olist_pipeline():
    bronze=DatabricksRunNowOperator(
        task_id="run_bronze",
        databricks_conn_id="olist_databricks",
        job_id=316458599069914,
        execution_timeout=timedelta(minutes=30)
    )
    silver = DatabricksRunNowOperator(
        task_id="run_silver",
        databricks_conn_id="olist_databricks",
        job_id=195334250053400,
        execution_timeout=timedelta(minutes=30)
    )
    data_quality_gate = DatabricksRunNowOperator(
        task_id="run_data_quality_gate",
        databricks_conn_id="olist_databricks",
        job_id=998522865501075,
        execution_timeout=timedelta(minutes=30)
    )
    gold = DatabricksRunNowOperator(
        task_id="run_gold",
        databricks_conn_id="olist_databricks",
        job_id=154324871188638,
        execution_timeout=timedelta(minutes=30)
    )
    bronze >> silver >> data_quality_gate >> gold

olist_pipeline()


