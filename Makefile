# Developer entrypoints. `make help` lists targets. Settings come from .env when present.
-include .env
export

UV ?= uv
MATCH ?= 1473511
SEASON ?= 2025

.DEFAULT_GOAL := help
.PHONY: help install lint format test test-unit test-spark test-integration batch silver gold dq \
        topics produce stream stream-once verify sql report dashboard up down logs compose-batch \
        compose-replay compose-verify clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-18s %s\n", $$1, $$2}'

install: ## Create the virtualenv with dev + dashboard extras
	$(UV) sync --all-extras

lint: ## Ruff lint + format check
	$(UV) run ruff check .
	$(UV) run ruff format --check .

format: ## Auto-fix lint and format
	$(UV) run ruff check --fix .
	$(UV) run ruff format .

test-unit: ## Fast tests (no JVM)
	$(UV) run pytest tests/unit

test-spark: ## Spark/Delta tests on real fixture matches (needs Java 17+)
	$(UV) run pytest tests/spark

test-integration: ## Kafka round trip (needs a broker at IPL_KAFKA_BOOTSTRAP_SERVERS)
	$(UV) run pytest -m integration tests/integration

test: ## Everything except the Kafka integration test
	$(UV) run pytest -m "not integration" --cov

batch: ## ingest -> silver -> DQ -> gold -> DQ
	$(UV) run ipl batch

silver: ## Incremental bronze -> silver
	$(UV) run ipl silver

gold: ## Rebuild gold marts
	$(UV) run ipl gold

dq: ## Run all data-quality checks
	$(UV) run ipl dq

topics: ## Create the Kafka topic
	$(UV) run ipl topics

produce: ## Replay MATCH into Kafka (SPEEDUP=60 by default)
	$(UV) run ipl produce --match-id $(MATCH)

stream: ## Run the streaming queries continuously
	$(UV) run ipl stream

stream-once: ## Process everything available in Kafka, then exit
	$(UV) run ipl stream --available-now

verify: ## Compare live_scorecard for MATCH with the batch view
	$(UV) run ipl verify-stream --match-id $(MATCH)

sql: ## Example ad-hoc query: SEASON's points table
	$(UV) run ipl sql "SELECT position, team, played, won, lost, no_result, points, net_run_rate FROM points_table WHERE season = $(SEASON) ORDER BY position"

report: ## Write the static results site to ./site (open site/index.html)
	$(UV) run ipl report --out site

dashboard: ## Streamlit dashboard on :8501
	$(UV) run ipl dashboard

up: ## docker compose: kafka + topic + stream processor
	docker compose up -d --build

down: ## docker compose: stop everything (keeps volumes)
	docker compose --profile ui --profile dashboard down

logs: ## Follow the stream processor logs
	docker compose logs -f stream

compose-batch: ## Backfill inside Docker
	docker compose run --rm batch

compose-replay: ## Replay SEASON inside Docker
	IPL_REPLAY_SEASON=$(SEASON) docker compose run --rm producer

compose-verify: ## Reconcile MATCH inside Docker
	docker compose run --rm stream verify-stream --match-id $(MATCH)

clean: ## Remove local caches (not data)
	rm -rf .pytest_cache .ruff_cache .coverage coverage.xml htmlcov
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
