.PHONY: setup postgres ingest dbt_run dbt_test airflow_start

setup:
	sudo apt update && sudo apt upgrade -y
	sudo apt install -y python3 python3-venv python3-pip libpq-dev
	sudo apt install postgresql postgresql-contrib
	python -m venv airflow-env && source airflow-env/bin/activate && pip install -r requirements.txt

postgres:
	sudo service postgresql start
	sudo -i -u postg
	psql

airflow_start:
	export AIRFLOW_HOME=~/airflow
	airflow db init
	airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
	airflow scheduler
	airflow webserver --port 8080

ingest:
	python data_ingestion/src/main.py

dbt_run:
	cd dbt_pipeline && dbt run

dbt_test:
	cd dbt_pipeline && dbt test