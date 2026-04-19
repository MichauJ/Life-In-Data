import duckdb
import os

def transform_data():
    db_path = "data/final/life_in_data.duckdb"
    os.makedirs("data/final", exist_ok=True)
    con = duckdb.connect(db_path)
    
    print("--- 🏗️ BUDOWANIE MODELU RELACYJNEGO ---")

    # 1. TABELA FAKTÓW: StayFree (Szczegółowe użycie aplikacji)
    print("Przetwarzanie faktów: StayFree...")
    con.execute("""
        CREATE OR REPLACE TABLE fact_stayfree AS
        SELECT 
            date, 
            aplikacja, 
            urządzenie, 
            usage_minutes 
        FROM read_csv_auto('data/raw/stayfree/stayfree_usage.csv')
    """)

    # 2. TABELA FAKTÓW: Habits (Nawyki z Twoją logiką przeliczania)
    print("Przetwarzanie faktów: Habits...")
    con.execute("""
        CREATE OR REPLACE TABLE fact_habits AS
        SELECT 
            date,
            habit_name,
            CASE WHEN habit_type = 0 THEN 'Tak/Nie' ELSE 'Mierzalny' END AS type_label,
            habit_unit,
            CASE 
                WHEN habit_type = 0 THEN habit_value 
                WHEN habit_type = 1 THEN habit_value / 1000.0 
                ELSE 0 
            END AS result_value
        FROM read_csv_auto('data/raw/habits/habits_history.csv')
    """)

    # 3. TABELA WYMIARÓW: Kalendarz
    # Generujemy daty od początku 2024 do końca 2026 (możesz zmienić zakres)
    print("Generowanie wymiaru: Kalendarz...")
    con.execute("""
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
            FROM range(DATE '2024-01-01', DATE '2027-01-01', INTERVAL 1 DAY)
        )
    """)

    # Usuwamy starą master_table, jeśli istniała, żeby nie śmieciła
    con.execute("DROP TABLE IF EXISTS master_table")

    print(f"Sukces! Baza zawiera teraz: fact_stayfree, fact_habits, dim_calendar.")
    con.close()

if __name__ == "__main__":
    transform_data()