.PHONY: build run shell logs clean

# Buduje obraz kontenera
build:
	docker compose build

# Uruchamia pełny pipeline ETL
run:
	docker compose run --rm etl_engine ./run_pipeline.sh

# Wchodzi do wnętrza kontenera (do debugowania)
shell:
	docker compose run --rm -it etl_engine /bin/bash

# Pokazuje ostatnie logi z pliku pipeline.log
logs:
	cat data/pipeline.log

# Czyści pliki tymczasowe i bazę (uwaga: usuwa dane!)
clean:
	rm -f data/raw/habits/*.parquet
	rm -f data/raw/stayfree/*.parquet
	rm -f data/final/*.duckdb