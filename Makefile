.PHONY: install test lint run backfill summary airflow-up airflow-down db sample

PY ?= python

install:            ## create venv deps
	$(PY) -m pip install -r requirements-dev.txt

lint:
	ruff check .

test:               ## unit + e2e tests (warehouse tests need TEST_PG_DSN)
	$(PY) -m pytest

run:                ## run today-1 end to end: make run DATE=2026-10-30
	$(PY) run_pipeline.py --date $(DATE)

backfill:           ## make backfill START=2026-10-01 END=2026-10-30
	$(PY) run_pipeline.py --start $(START) --end $(END)

summary:            ## MVP: daily engagement summary from the Bronze data in R2
	$(PY) -m ingestion.download_data --date $(DATE) --summary

db:                 ## create warehouse tables manually (the loader also does this)
	psql "$$PG_DSN" -f warehouse/schema.sql

sample:
	$(PY) -m scripts.make_sample_data

airflow-up:
	docker compose up airflow-init && docker compose up -d

airflow-down:
	docker compose down
