"""Airflow DAG for the daily gold-table build."""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

REPO = "{{ var.value.get('perishables_repo', '/opt/airflow/perishables-intelligence-platform') }}"


with DAG(
    dag_id="perishables_risk_scoring",
    description="Generate warehouse gold tables and run data-quality checks.",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["perishables", "dbt", "quality"],
) as dag:
    generate_sample_data = BashOperator(
        task_id="generate_sample_data",
        bash_command=(
            f"cd {REPO} && "
            "python data/generators/seed.py --stores 20 --skus 500 --days 30"
        ),
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"cd {REPO}/warehouse/dbt && "
            "PERISHABLES_DATA_DIR=../../data/generated "
            "dbt build --profiles-dir ."
        ),
    )

    validate_expectation_suites = BashOperator(
        task_id="validate_expectation_suites",
        bash_command=(
            f"cd {REPO} && "
            "python -m json.tool quality/great_expectations/expectations/inventory_snapshot_suite.json >/dev/null && "
            "python -m json.tool quality/great_expectations/expectations/perishables_risk_suite.json >/dev/null"
        ),
    )

    generate_sample_data >> dbt_build >> validate_expectation_suites
