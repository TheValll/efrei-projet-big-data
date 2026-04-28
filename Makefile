.PHONY: help up down restart clean logs api airflow postgres hdfs spark psql hdfs-ls pods push

NAMESPACE = efrei-big-data
ENV_FILE = $(shell [ -f .env ] && echo .env || echo .env.example)
COMPOSE = docker compose --env-file $(ENV_FILE)

help:
	@grep -E '^[a-zA-Z_-]+:.*?##.*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

up: ## Build and start the full stack (postgres, hdfs, airflow, api)
	$(COMPOSE) up -d --build

down: ## Stop and remove containers (keeps volumes)
	$(COMPOSE) down

restart: down up ## Restart the stack

clean: ## Stop and wipe all volumes (forces full re-init + CSV reload)
	$(COMPOSE) down -v

logs: ## Tail logs from all services
	$(COMPOSE) logs -f

airflow: ## Tail Airflow logs (UI: http://localhost:8080)
	$(COMPOSE) logs -f airflow

api: ## Tail API logs (http://localhost:5000)
	$(COMPOSE) logs -f api

postgres: ## Tail Postgres logs (localhost:5432)
	$(COMPOSE) logs -f postgres

hdfs: ## Tail namenode/datanode logs (UI: http://localhost:9870)
	$(COMPOSE) logs -f namenode datanode

spark: ## Tail Spark master/worker logs (UI: http://localhost:8081)
	$(COMPOSE) logs -f spark-master spark-worker

psql: ## psql shell on nasa_db_raw
	$(COMPOSE) exec postgres psql -U postgres -d nasa_db_raw

hdfs-ls: ## Recursively list /bronze on HDFS
	$(COMPOSE) exec namenode hdfs dfs -ls -R /bronze
