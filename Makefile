.PHONY: setup postgres ingest dbt_run dbt_test airflow_start

setup:
	sudo apt update && sudo apt install postgresql postgresql-contrib
	python -m venv airflow-env && source airflow-env/bin/activate && pip install -r requirements.txt



postgres:
	sudo service postgresql start
	sudo -i -u postg
	psql

ingest:
	python data_ingestion/src/ingest.py

dbt_run:
	cd dbt_pipeline && dbt run

dbt_test:
	cd dbt_pipeline && dbt test

airflow_start:

	airflow scheduler