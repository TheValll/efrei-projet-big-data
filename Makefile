.PHONY: help up down restart clean logs api airflow postgres hdfs spark psql hdfs-ls pods push dump restore

ENV_FILE = $(if $(wildcard .env),.env,.env.example)
COMPOSE = docker compose --env-file $(ENV_FILE)

help:
	@grep -E '^[a-zA-Z_-]+:.*?##.*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

up: ## Build and start the full stack (postgres, hdfs, airflow, api)
	$(COMPOSE) up -d --build

down: ## Stop and remove containers (keeps volumes)
	$(COMPOSE) down

dump: ## Dump the datamart database to dumps/datamart.sql
	-mkdir dumps
	$(COMPOSE) exec -T postgres pg_dump -U postgres -d datamart > dumps/datamart.sql
	@echo Dumped to dumps/datamart.sql

restore: ## Restore the datamart dump (usage: make restore DUMP=dumps/datamart.sql)
	$(COMPOSE) exec -T postgres psql -U postgres -d datamart < $(DUMP)
	@echo Restored from $(DUMP)
