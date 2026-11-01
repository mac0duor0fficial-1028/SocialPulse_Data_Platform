"""Daily SocialPulse ETL: generate -> upload_bronze -> transform_silver -> build_gold -> load_postgres.

Targets Apache Airflow 2.10.x (pinned in the Dockerfile). Every task derives its work from the
run's logical date (`ds`), writes to date-partitioned keys and upserts into the warehouse, so
clearing or re-running any task/date is safe (idempotent).
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# The project root is mounted at /opt/airflow/project in Docker (also works for a local checkout).
_ROOT = Path(__file__).resolve().parents[1]
for candidate in (Path("/opt/airflow/project"), _ROOT):
    if candidate.exists() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

log = logging.getLogger("socialpulse.dag")


def _alert_on_failure(context) -> None:
    """Failure alert hook. Logs prominently and appends to logs/alerts.log.
    Swap the body for Slack / email / PagerDuty in a real deployment."""
    ti = context["task_instance"]
    msg = (f"ALERT: {ti.dag_id}.{ti.task_id} failed for {context.get('ds')} "
           f"(try {ti.try_number}): {context.get('exception')}")
    log.error(msg)
    from config.settings import PROJECT_ROOT
    path = PROJECT_ROOT / "logs" / "alerts.log"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(f"{datetime.utcnow().isoformat()}Z {msg}\n")


def _sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    log.error("SLA missed for tasks: %s", task_list)


# Imports of project code are deferred into the callables so DAG parsing stays fast and does
# not require credentials to be present at parse time.
def _generate(ds: str, **_):
    from config.settings import Settings
    from data_generation.run_generation import generate_all
    generate_all(Settings.from_env(), ds)


def _upload_bronze(ds: str, **_):
    from config.settings import Settings
    from ingestion.upload_data import upload_run
    upload_run(Settings.from_env(), ds)


def _transform_silver(ds: str, **_):
    from config.settings import Settings
    from transformations.run_silver import transform_silver
    transform_silver(Settings.from_env(), ds)


def _build_gold(ds: str, **_):
    from config.settings import Settings
    from transformations.build_gold import build_gold
    build_gold(Settings.from_env(), ds)


def _load_postgres(ds: str, **_):
    from config.settings import Settings
    from warehouse.load_gold import load_warehouse
    load_warehouse(Settings.from_env(), ds)


default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(minutes=45),
    "sla": timedelta(hours=2),
    "on_failure_callback": _alert_on_failure,
}

with DAG(
    dag_id="socialpulse_pipeline",
    description="Synthetic social analytics: R2 medallion lake -> PostgreSQL star schema",
    default_args=default_args,
    start_date=datetime(2026, 10, 1),
    schedule="@daily",
    catchup=False,            # trigger backfills explicitly: `airflow dags backfill`
    max_active_runs=1,        # chronological loads: required for SCD2 and growth rates
    sla_miss_callback=_sla_miss,
    tags=["socialpulse", "etl", "portfolio"],
    doc_md=__doc__,
) as dag:
    generate = PythonOperator(task_id="generate", python_callable=_generate)
    upload_bronze = PythonOperator(task_id="upload_bronze", python_callable=_upload_bronze)
    transform_silver = PythonOperator(task_id="transform_silver", python_callable=_transform_silver)
    build_gold = PythonOperator(task_id="build_gold", python_callable=_build_gold)
    load_postgres = PythonOperator(task_id="load_postgres", python_callable=_load_postgres)

    generate >> upload_bronze >> transform_silver >> build_gold >> load_postgres
