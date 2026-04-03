.PHONY: up down restart logs test backfill clean

up:
	docker compose up -d
	@echo "Airflow UI: http://localhost:8080 (admin/admin)"

down:
	docker compose down

restart:
	docker compose down && docker compose up -d

logs:
	docker compose logs -f airflow-scheduler airflow-worker

test:
	docker compose run --rm airflow-worker python -m pytest /opt/airflow/tests/ -v --tb=short

backfill:
	@if [ -z "$(YEAR)" ]; then echo "Usage: make backfill YEAR=2020"; exit 1; fi
	docker compose run --rm airflow-worker airflow dags backfill \
		-s $(YEAR)-01-01 -e $(YEAR)-12-31 \
		--reset-dagruns \
		$(DAG)

build:
	docker compose build --no-cache

clean:
	docker compose down -v
	rm -rf logs/ data/raw/ data/bronze/ data/silver/ data/gold/ warehouse/*.duckdb
