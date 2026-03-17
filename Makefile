.PHONY: setup run extract load transform quality test lint format clean help

# Default target
help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

setup: ## Install all dependencies
	python -m pip install --upgrade pip
	pip install -r requirements.txt
	pip install -r requirements-dev.txt
	@echo ""
	@echo "Setup complete. Run 'make run' to execute the pipeline."

# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

run: extract load quality ## Run the full pipeline (extract -> load -> quality)
	@echo ""
	@echo "Pipeline complete."

extract: ## Extract source data from public datasets
	python -m pipeline.extract

load: ## Load extracted data into warehouse (DuckDB)
	python -m pipeline.load

transform: ## Run dbt transformations
	dbt run

quality: ## Run data quality checks
	python -m pipeline.quality

orchestrate: ## Run the full orchestrated pipeline with retry and monitoring
	python -m pipeline.orchestrate

orchestrate-dry: ## Dry run of the orchestrated pipeline
	python -m pipeline.orchestrate --dry-run

# ---------------------------------------------------------------------------
# Development
# ---------------------------------------------------------------------------

test: ## Run all tests
	pytest tests/ -v --tb=short

test-cov: ## Run tests with coverage report
	pytest tests/ -v --tb=short --cov=pipeline --cov-report=term-missing --cov-report=html

lint: ## Lint and type-check
	ruff check pipeline/ tests/
	ruff format --check pipeline/ tests/
	mypy pipeline/ --ignore-missing-imports

format: ## Auto-format code
	ruff format pipeline/ tests/
	ruff check --fix pipeline/ tests/

# ---------------------------------------------------------------------------
# dbt
# ---------------------------------------------------------------------------

dbt-compile: ## Compile dbt models (syntax check)
	dbt compile

dbt-run: ## Run dbt models
	dbt run

dbt-test: ## Run dbt tests
	dbt test

dbt-docs: ## Generate and serve dbt documentation
	dbt docs generate
	dbt docs serve

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

clean: ## Remove generated files
	rm -rf data/
	rm -rf target/
	rm -rf dbt_packages/
	rm -rf .pytest_cache/
	rm -rf htmlcov/
	rm -rf .mypy_cache/
	rm -rf .ruff_cache/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	@echo "Cleaned all generated files."
