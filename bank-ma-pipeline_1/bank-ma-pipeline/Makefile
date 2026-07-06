# ─────────────────────────────────────────────────────────────
# Bank M&A Data Migration Pipeline — Make shortcuts
# Run `make help` to see all available commands.
# ─────────────────────────────────────────────────────────────

.PHONY: help up down restart logs clean test test-unit \
        generate-data dbt-deps dbt-run dbt-test run-local airflow-ui

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

up: ## Start all containers (Postgres x2, Airflow, data generator)
	docker-compose up -d
	@echo "Waiting for services to become healthy..."
	@sleep 5
	@echo "Airflow UI will be available at http://localhost:8080 (admin/admin)"
	@echo "It can take 1-2 minutes for Airflow to fully initialise."

down: ## Stop all containers
	docker-compose down

restart: down up ## Restart the full stack

logs: ## Tail logs from all containers
	docker-compose logs -f

logs-airflow: ## Tail Airflow scheduler logs only
	docker-compose logs -f airflow-scheduler

clean: ## Stop containers AND delete all data volumes (full reset)
	docker-compose down -v
	@echo "All data volumes removed. Next 'make up' starts from a blank state."

test: test-unit ## Run all tests

test-unit: ## Run Python unit tests (no database required)
	pip install -r requirements-dev.txt --break-system-packages --quiet
	pytest tests/ -v --tb=short

generate-data: ## Generate synthetic loan data into the running source DB
	cd data_generator && python generate_loans.py

dbt-deps: ## Install dbt packages (dbt-utils)
	cd dbt_project && dbt deps --profiles-dir .

dbt-run: ## Run all dbt models (raw + clean layers)
	cd dbt_project && dbt run --profiles-dir .

dbt-test: ## Run all dbt schema quality tests
	cd dbt_project && dbt test --profiles-dir .

dbt-docs: ## Generate and serve dbt documentation (data lineage graph)
	cd dbt_project && dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir .

run-local: ## Run the full pipeline locally without Airflow (requires `make up` first)
	python run_pipeline.py --records 100000

run-local-full: ## Run the full pipeline with the full 5M record dataset
	python run_pipeline.py --records 5000000

airflow-ui: ## Open the Airflow UI in your default browser
	@echo "Opening http://localhost:8080 ..."
	@python3 -c "import webbrowser; webbrowser.open('http://localhost:8080')" || \
		echo "Please open http://localhost:8080 manually"

psql-source: ## Open a psql shell into the Source (Bank B legacy) database
	docker exec -it bank_b_legacy psql -U bankuser -d bank_b_legacy

psql-target: ## Open a psql shell into the Target (Bank A DWH) database
	docker exec -it bank_a_dwh psql -U bankuser -d bank_a_dwh
