.PHONY: help install api pods logs push

NAMESPACE = efrei-big-data

help:
	@grep -E '^[a-zA-Z_-]+:.*?##.*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

install: ## Install Python dependencies for the API
	pip install -r api/requirements.txt

api: ## Run the Flask API locally on port 5000
	python api/api.py