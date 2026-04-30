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

