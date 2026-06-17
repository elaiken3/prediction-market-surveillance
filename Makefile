.PHONY: up down topics live synth stream resolutions dbt test dash clean

up:
	docker compose up -d
down:
	docker compose down -v
topics:
	docker compose exec redpanda rpk topic create markets.events -p 6 || true
	docker compose exec redpanda rpk topic create markets.dlq -p 1 || true

# Live Polymarket ingestion (no auth needed for market data)
live:
	python -m ingestion.polymarket_ws --bootstrap localhost:19092 --topic markets.events --markets 8

# Offline synthetic stream (no network) -- great for dev/CI/demos
synth:
	python -m ingestion.synthetic --bootstrap localhost:19092 --topic markets.events \
		--count 5000 --rate 25 --manip-rate 0.05 --dirty-rate 0.05 --seed 42

preview:
	python -m ingestion.synthetic --dry-run --count 15 --manip-rate 0.2 --dirty-rate 0.2 --seed 1

stream:
	spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 streaming/spark_job.py

resolutions:
	python -m ingestion.fetch_resolutions --lake data/lake --limit 200

dbt:
	mkdir -p dbt/market_surveillance/data
	cp -n dbt/market_surveillance/profiles.example.yml dbt/market_surveillance/profiles.yml || true
	python -m ingestion.seed_lake --lake $(PWD)/data/lake
	cd dbt/market_surveillance && DBT_PROFILES_DIR=$(PWD)/dbt/market_surveillance dbt deps && \
		DBT_PROFILES_DIR=$(PWD)/dbt/market_surveillance dbt build --vars '{lake_path: $(PWD)/data/lake}'

test:
	python -m pytest -q

dash:
	streamlit run dashboard/app.py

clean:
	rm -rf data/lake data/checkpoints data/warehouse.duckdb dbt/market_surveillance/target