all: airflow_up

airflow_up:
	docker-compose -f ./airflow/docker-compose.yaml up -d