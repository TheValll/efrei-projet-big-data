.PHONY: help api airflow pods logs push

NAMESPACE = efrei-big-data
ENV_FILE = $(shell [ -f .env ] && echo .env || echo .env.example)

help:
	@grep -E '^[a-zA-Z_-]+:.*?##.*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

api: ## Build and run the API in Docker on port 5000
	docker build -f infra/Dockerfile.api -t efrei-api:local .
	docker run --rm -it --env-file $(ENV_FILE) -p 5000:5000 --name efrei-api efrei-api:local

airflow: ## Build and run Airflow standalone in Docker on port 8080
	docker build -f infra/Dockerfile.airflow -t efrei-airflow:local .
	docker run --rm -it --env-file $(ENV_FILE) -p 8080:8080 --name efrei-airflow efrei-airflow:local