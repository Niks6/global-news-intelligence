# Global News Intelligence Platform — Makefile
# Common developer commands. Run from project root.

.PHONY: install test lint backend-dev frontend-install frontend-dev ingest-once ingest-poll \
        create-kafka-topics create-hbase-tables download-geo

# ---- Python ----
install:
	pip install -r requirements.txt

test:
	pytest src/ backend/ tests/ -v --tb=short

lint:
	python -m flake8 src/ backend/ --max-line-length 120 --ignore E501,W503

# ---- Backend ----
backend-dev:
	uvicorn backend.main:app --reload --host 0.0.0.0 --port 8000

# ---- Frontend ----
frontend-install:
	cd frontend && npm install

frontend-dev:
	cd frontend && npm run dev

frontend-build:
	cd frontend && npm run build

# ---- Ingestion ----
ingest-once:
	python -m src.ingestion.cli once

ingest-poll:
	python -m src.ingestion.cli poll

ingest-status:
	python -m src.ingestion.cli status

# ---- Infrastructure ----
create-kafka-topics:
	bash infrastructure/kafka/create-topics.sh

create-hbase-tables:
	python -c "from src.storage.hbase_schema import create_all_tables; create_all_tables()"

download-geo:
	bash scripts/download-geo-data.sh

# ---- Full start (local dev) ----
dev:
	@echo "Start backend and frontend in separate terminals:"
	@echo "  Terminal 1: make backend-dev"
	@echo "  Terminal 2: make frontend-dev"

