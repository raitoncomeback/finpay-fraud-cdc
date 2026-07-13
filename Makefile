# FinPay Fraud CDC Pipeline - Makefile
# Common commands for development and deployment

.PHONY: help up down restart logs ps health generate-data register-debezium run-risingwave-dbt run-dbt test lint clean

# Default target
help:
	@echo "FinPay Fraud CDC Pipeline - Available Commands:"
	@echo ""
	@echo "Infrastructure:"
	@echo "  make up                 Start all services (Docker Compose)"
	@echo "  make down               Stop all services"
	@echo "  make restart            Restart all services"
	@echo "  make logs               View logs (use SERVICE=name for specific)"
	@echo "  make ps                 Show service status"
	@echo "  make health             Run health checks"
	@echo ""
	@echo "Data Pipeline:"
	@echo "  make init-db            Initialize FinPay Postgres schema"
	@echo "  make generate-data      Generate synthetic transaction data"
	@echo "  make register-debezium  Register Debezium CDC connector"
	@echo "  make risingwave-ddl     Create RisingWave sources, tables, MVs"
	@echo "  make run-dbt            Run dbt models (staging -> intermediate -> marts)"
	@echo "  make dbt-test           Run dbt tests"
	@echo "  make full-pipeline      Run complete pipeline (init -> generate -> debezium -> risingwave -> dbt)"
	@echo ""
	@echo "Development:"
	@echo "  make test               Run unit tests"
	@echo "  make lint               Run linting (black, isort, flake8)"
	@echo "  make format             Format code (black, isort)"
	@echo "  make clean              Clean up generated files and Docker volumes"
	@echo ""

# ==================== INFRASTRUCTURE ====================

up:
	docker-compose up -d --build

down:
	docker-compose down

restart:
	docker-compose restart

logs:
	docker-compose logs -f $(SERVICE)

ps:
	docker-compose ps

health:
	python scripts/health_check.py

# ==================== DATA PIPELINE ====================

init-db:
	docker exec -i finpay-postgres psql -U finpay -d finpay < scripts/init_finpay.sql
	@echo "✅ Database initialized"

generate-data:
	python scripts/generate_finpay_data.py --transactions 50000
	@echo "✅ Synthetic data generated"

register-debezium:
	@chmod +x scripts/register_debezium.sh
	./scripts/register_debezium.sh
	@echo "✅ Debezium connector registered"

risingwave-ddl:
	docker exec -i finpay-risingwave psql -h localhost -p 4566 -U root -d dev < risingwave/ddl_bronze.sql
	docker exec -i finpay-risingwave psql -h localhost -p 4566 -U root -d dev < risingwave/ddl_silver.sql
	docker exec -i finpay-risingwave psql -h localhost -p 4566 -U root -d dev < risingwave/ddl_gold.sql
	@echo "✅ RisingWave DDL executed"

run-dbt:
	cd dbt_finpay && dbt deps && dbt run --profiles-dir .

dbt-test:
	cd dbt_finpay && dbt test --profiles-dir .

# Full pipeline from scratch
full-pipeline: init-db generate-data register-debezium risingwave-ddl run-dbt dbt-test
	@echo "✅ Full pipeline completed successfully!"

# ==================== DEVELOPMENT ====================

test:
	python -m pytest tests/ -v --cov=src --cov-report=term-missing

lint:
	black --check --diff .
	isort --check-only --diff .
	flake8 . --max-line-length=100 --ignore=E203,W503

format:
	black .
	isort .

# ==================== CLEANUP ====================

clean:
	docker-compose down -v
	docker system prune -f
	rm -rf dbt_finpay/target
	rm -rf dbt_finpay/dbt_packages
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	@echo "✅ Cleanup complete"

# ==================== AIRFLOW ====================

airflow-init:
	docker-compose up airflow-init

airflow-webserver:
	docker-compose up -d airflow-webserver

airflow-scheduler:
	docker-compose up -d airflow-scheduler

# ==================== MONITORING ====================

prometheus:
	docker-compose up -d prometheus

grafana:
	docker-compose up -d grafana

# ==================== FRAUD API ====================

api-build:
	docker build -t finpay-fraud-api ./fraud_features

api-run:
	docker-compose up -d fraud-api

api-logs:
	docker-compose logs -f fraud-api

# ==================== UTILITIES ====================

# Show Kafka topics
kafka-topics:
	docker exec finpay-kafka kafka-topics --bootstrap-server localhost:9092 --list

# Show Kafka consumer groups
kafka-groups:
	docker exec finpay-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --list

# Show RisingWave materialized views
rw-mvs:
	docker exec finpay-risingwave psql -h localhost -p 4566 -U root -d dev -c "SELECT name, definition FROM rw_materialized_views;"

# Show Iceberg tables
iceberg-tables:
	docker exec finpay-risingwave psql -h localhost -p 4566 -U root -d dev -c "SHOW TABLES;"

# Query sample data
sample-data:
	docker exec finpay-risingwave psql -h localhost -p 4566 -U root -d dev -c "SELECT * FROM gold_fraud_features ORDER BY composite_risk_score DESC LIMIT 10;"

# Count records
counts:
	docker exec finpay-risingwave psql -h localhost -p 4566 -U root -d dev -c "
	SELECT 'bronze_transactions' as table, COUNT(*) as cnt FROM bronze_transactions
	UNION ALL SELECT 'silver_transactions_enriched', COUNT(*) FROM silver_transactions_enriched
	UNION ALL SELECT 'gold_fraud_features', COUNT(*) FROM gold_fraud_features
	UNION ALL SELECT 'gold_user_fraud_features', COUNT(*) FROM gold_user_fraud_features
	UNION ALL SELECT 'gold_merchant_risk_score', COUNT(*) FROM gold_merchant_risk_score
	UNION ALL SELECT 'gold_device_risk_score', COUNT(*) FROM gold_device_risk_score;
	"