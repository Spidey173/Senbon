.PHONY: help setup run-pipeline test test-unit test-integration up down restart clean logs dbt-run

help:
	@echo "Senbon — Retail Intelligence & Analytics Engine - Management Commands:"
	@echo "  make setup            Install Python dependencies and setup local directories"
	@echo "  make run-pipeline     Execute the complete 5-stage lakehouse pipeline locally"
	@echo "  make test             Execute all unit and integration tests with pytest"
	@echo "  make up               Launch full containerized platform via Docker Compose"
	@echo "  make down             Stop and remove Docker containers"
	@echo "  make logs             Stream container logs"
	@echo "  make clean            Remove local data artifacts, sqlite replicas, and caches"

setup:
	pip install -r requirements.txt
	mkdir -p data/raw data/validated data/rejected data/bronze data/silver data/gold logs

run-pipeline:
	PYTHONPATH=. python3 scripts/run_pipeline.py

test:
	PYTHONPATH=. pytest tests/ -v

test-unit:
	PYTHONPATH=. pytest tests/unit/ -v

test-integration:
	PYTHONPATH=. pytest tests/integration/ -v

up:
	docker compose up -d --build
	@echo "Services launching..."
	@echo "Airflow Web UI:      http://localhost:8080 (admin / admin)"
	@echo "MinIO Console:       http://localhost:9001 (minioadmin / minioadmin123)"
	@echo "Retail REST APIs:    http://localhost:8000/docs"
	@echo "Metabase BI:         http://localhost:3000"

down:
	docker compose down -v

restart: down up

logs:
	docker compose logs -f

clean:
	rm -rf data/raw/* data/validated/* data/rejected/* data/bronze/* data/silver/* data/gold/*
	rm -rf data/*.duckdb* data/*.sqlite data/*.db
	rm -rf logs/*.log .pytest_cache
	find . -type d -name "__pycache__" -exec rm -rf {} +

