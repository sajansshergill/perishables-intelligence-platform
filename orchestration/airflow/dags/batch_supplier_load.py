"""Airflow DAG for daily supplier and shelf-life batch loads."""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

S3_BUCKET = "{{ var.value.get('perishables_bucket', 'perishables-lake') }}"


with DAG(
    dag_id="batch_supplier_load",
    description="Conform supplier and shelf-life reference extracts with Glue.",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["perishables", "glue", "batch"],
) as dag:
    load_suppliers = GlueJobOperator(
        task_id="load_suppliers",
        job_name="perishables-supplier-load",
        script_args={
            "--source_path": f"s3://{S3_BUCKET}/landing/suppliers/dt={{{{ ds }}}}/",
            "--target_path": f"s3://{S3_BUCKET}/bronze/suppliers/",
            "--load_date": "{{ ds }}",
        },
    )

    load_shelf_life = GlueJobOperator(
        task_id="load_shelf_life",
        job_name="perishables-shelf-life-load",
        script_args={
            "--source_path": f"s3://{S3_BUCKET}/landing/shelf_life/dt={{{{ ds }}}}/",
            "--target_path": f"s3://{S3_BUCKET}/bronze/shelf_life/",
        },
    )

    [load_suppliers, load_shelf_life]
