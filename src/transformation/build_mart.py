import duckdb
import os
from src.utils.config_loader import cfg
from src.utils.logger_setup import setup_logger
import sys
# Inicjalizacja loggera dla etapu transformacji
logger = setup_logger("transform-mart")

def transform_data():
    """Tworzy model danych w DuckDB: fakty StayFree, fakty Habits oraz kalendarz."""
    
    db_path = cfg['paths']['final_db']
    habits_csv = cfg['paths']['habits_raw_csv']
    stayfree_csv = cfg['paths']['stayfree_raw_csv']
    
    divider = cfg['logic']['habit_value_divider']
    start_date = cfg['logic']['calendar_start_date']
    end_date = cfg['logic']['calendar_end_date']

    logger.info("Rozpoczynam etap transformacji w DuckDB...")
    
    # Upewniamy się, że folder docelowy istnieje
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    # Łączymy się z bazą DuckDB
    con = duckdb.connect(db_path)
    
    try:
        # 1. TABELA FAKTÓW: StayFree
        logger.info(f"Budowanie tabeli fact_stayfree...")
        con.execute(f"""
            CREATE OR REPLACE TABLE fact_stayfree AS
            SELECT 
                CAST(date AS DATE) AS date,  -- <--- JAWNE RZUTOWANIE NA DATĘ
                aplikacja, 
                urządzenie, 
                usage_minutes 
            FROM read_csv_auto('{stayfree_csv}')
        """)

        # 2. TABELA FAKTÓW: Habits
        logger.info(f"Budowanie tabeli fact_habits...")
        con.execute(f"""
            CREATE OR REPLACE TABLE fact_habits AS
            SELECT 
                CAST(date AS DATE) AS date,  -- <--- JAWNE RZUTOWANIE NA DATĘ
                habit_name,
                CASE WHEN habit_type = 0 THEN 'Tak/Nie' ELSE 'Mierzalny' END AS type_label,
                habit_unit,
                CASE 
                    WHEN habit_type = 0 THEN habit_value 
                    WHEN habit_type = 1 THEN habit_value / {divider}
                    ELSE 0 
                END AS result_value
            FROM read_csv_auto('{habits_csv}')
        """)

        # 3. TABELA WYMIARÓW: Kalendarz
        logger.info(f"Generowanie wymiaru dim_calendar ({start_date} do {end_date})")
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

        # Czyszczenie starej architektury
        con.execute("DROP TABLE IF EXISTS master_table")

        # LOGOWANIE STATYSTYK (Data Quality Check)
        stats_stayfree = con.execute("SELECT COUNT(*) FROM fact_stayfree").fetchone()[0]
        stats_habits = con.execute("SELECT COUNT(*) FROM fact_habits").fetchone()[0]
        stats_calendar = con.execute("SELECT COUNT(*) FROM dim_calendar").fetchone()[0]

        logger.info("Transformacja zakończona sukcesem.")
        logger.info(f"Statystyki bazy: fact_stayfree ({stats_stayfree} wierszy), "
                    f"fact_habits ({stats_habits} wierszy), "
                    f"dim_calendar ({stats_calendar} dni)")

    except Exception as e:
        logger.error(f"KRYTYCZNY BŁĄD PODCZAS TRANSFORMACJI: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        con.close()
        logger.info("Połączenie z DuckDB zostało zamknięte.")

if __name__ == "__main__":
    transform_data()