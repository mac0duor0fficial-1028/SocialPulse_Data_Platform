"""DAG integrity test (skipped automatically when Airflow is not installed, e.g. in CI's slim env)."""
import os
import tempfile
from datetime import timedelta
from pathlib import Path

import pytest

# Must be set before airflow is imported: use a throw-away sqlite metadata DB.
_HOME = tempfile.mkdtemp(prefix="airflow_test_")
os.environ["AIRFLOW_HOME"] = _HOME
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "false"
os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = f"sqlite:///{_HOME}/airflow.db"

pytest.importorskip("airflow")
from airflow.models import DagBag  # noqa: E402
from airflow.utils.db import initdb  # noqa: E402

DAG_DIR = Path(__file__).resolve().parents[1] / "dags"


@pytest.fixture(scope="module")
def dag():
    initdb()
    bag = DagBag(dag_folder=str(DAG_DIR), include_examples=False)
    assert not bag.import_errors, bag.import_errors
    return bag.get_dag("socialpulse_pipeline")


def test_task_chain(dag):
    order = ["generate", "upload_bronze", "transform_silver", "build_gold", "load_postgres"]
    assert [t.task_id for t in dag.topological_sort()] == order


def test_reliability_settings(dag):
    for task in dag.tasks:
        assert task.retries == 3
        assert task.retry_delay == timedelta(minutes=5)
        assert task.sla == timedelta(hours=2)
        assert task.on_failure_callback is not None
    assert dag.max_active_runs == 1 and dag.catchup is False
    assert dag.schedule_interval == "@daily"
