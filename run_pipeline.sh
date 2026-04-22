#!/bin/bash

# "Fail-fast" - przerwij skrypt, jeśli którakolwiek komenda zwróci błąd
set -e

echo "🚀 Rozpoczynam pełny proces ETL: Life-In-Data"
echo "--------------------------------------------"

# 1. Ingestia danych (Raw Layer)
echo "📥 [1/4] Pobieranie danych: Habits..."
PYTHONPATH=. python3 src/ingestion/habits.py

echo "📥 [2/4] Pobieranie danych: StayFree..."
PYTHONPATH=. python3 src/ingestion/stayfree.py

# 2. Transformacja (Mart Layer)
echo "🏗️ [3/4] Budowanie modelu DuckDB..."
PYTHONPATH=. python3 src/transformation/build_mart.py

# 3. Jakość danych (Quality Gate)
echo "🔍 [4/4] Uruchamianie testów jakości (Data Quality)..."
PYTHONPATH=. python3 src/quality/dq_suite.py

echo "--------------------------------------------"
echo "✅ Pipeline zakończony sukcesem! Dane są gotowe."