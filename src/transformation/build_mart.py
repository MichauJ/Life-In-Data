import duckdb
import os
from src.utils.config_loader import cfg

def transform_data():
    # Pobieramy ścieżki i parametry z centralnej konfiguracji
    db_path = cfg['paths']['final_db']
    habits_csv = cfg['paths']['habits_raw_csv']
    stayfree_csv = cfg['paths']['stayfree_raw_csv']
    
    divider = cfg['logic']['habit_value_divider']
    start_date = cfg['logic']['calendar_start_date']
    end_date = cfg['logic']['calendar_end_date']

    # Upewniamy się, że folder docelowy istnieje
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    # Łączymy się z bazą DuckDB
    con = duckdb.connect(db_path)
    
    print("\n--- 🏗️ ETAP TRANSFORMACJI (DuckDB) ---")

    try:
        # 1. TABELA FAKTÓW: StayFree (Szczegółowe użycie aplikacji)
        print(f"Przetwarzanie faktów: {os.path.basename(stayfree_csv)}...")
        con.execute(f"""
            CREATE OR REPLACE TABLE fact_stayfree AS
            SELECT 
                date, 
                aplikacja, 
                urządzenie, 
                usage_minutes 
            FROM read_csv_auto('{stayfree_csv}')
        """)

        # 2. TABELA FAKTÓW: Habits (Nawyki z logiką typów i skalowaniem)
        print(f"Przetwarzanie faktów: {os.path.basename(habits_csv)}...")
        con.execute(f"""
            CREATE OR REPLACE TABLE fact_habits AS
            SELECT 
                date,
                habit_name,
                CASE WHEN habit_type = 0 THEN 'Tak/Nie' ELSE 'Mierzalny' END AS type_label,
                habit_unit,
                -- Korekta wartości: mierzalne dzielimy przez {divider}, binarne zostawiamy
                CASE 
                    WHEN habit_type = 0 THEN habit_value 
                    WHEN habit_type = 1 THEN habit_value / {divider}
                    ELSE 0 
                END AS result_value
            FROM read_csv_auto('{habits_csv}')
        """)

        # 3. TABELA WYMIARÓW: Kalendarz (dim_calendar)
        print(f"Generowanie wymiaru kalendarza ({start_date} do {end_date})...")
        con.execute(f"""
            CREATE OR REPLACE TABLE dim_calendar AS
            SELECT
                datum AS date,
                year(datum) AS rok,
                quarter(datum) AS kwartal,
                month(datum) AS miesiac,
                monthname(datum) AS nazwa_miesiaca,
                day(datum) AS dzien,
                dayofweek(datum) AS dzien_tygodnia_num,
                CASE dayofweek(datum)
                    WHEN 0 THEN 'Niedziela'
                    WHEN 1 THEN 'Poniedziałek'
                    WHEN 2 THEN 'Wtorek'
                    WHEN 3 THEN 'Środa'
                    WHEN 4 THEN 'Czwartek'
                    WHEN 5 THEN 'Piątek'
                    WHEN 6 THEN 'Sobota'
                END AS dzien_tygodnia
            FROM (
                SELECT CAST(range AS DATE) AS datum
                FROM range(DATE '{start_date}', DATE '{end_date}', INTERVAL 1 DAY)
            )
        """)

        # Usuwamy starą tabelę master_table (pozostałość po poprzedniej architekturze)
        con.execute("DROP TABLE IF EXISTS master_table")

        print(f"✓ Sukces! Baza {os.path.basename(db_path)} gotowa.")
        
        # Szybka kontrola jakości w logach
        row_count = con.execute("SELECT COUNT(*) FROM fact_stayfree").fetchone()[0]
        print(f"  [Statystyki] fact_stayfree: {row_count} wierszy")
        
        row_count_habits = con.execute("SELECT COUNT(*) FROM fact_habits").fetchone()[0]
        print(f"  [Statystyki] fact_habits: {row_count_habits} wierszy")

    except Exception as e:
        print(f"❌ BŁĄD PODCZAS TRANSFORMACJI: {e}")
    
    finally:
        con.close()

if __name__ == "__main__":
    transform_data()