import duckdb
import sys
from src.utils.config_loader import cfg
from src.utils.logger_setup import setup_logger

logger = setup_logger("data-quality")

def run_dq_checks():
    db_path = cfg['paths']['final_db']
    con = None
    
    try:
        con = duckdb.connect(db_path)
        logger.info("--- Rozpoczynam audyt jakości danych ---")
        
        # Definicja testów
        checks = [
            (
                "Brakujące daty w StayFree",
                "SELECT count(*) FROM fact_stayfree WHERE date IS NULL"
            ),
            (
                "Duplikaty w nawykach",
                """SELECT count(*) FROM (
                    SELECT date, habit_name, count(*) 
                    FROM fact_habits 
                    GROUP BY date, habit_name 
                    HAVING count(*) > 1
                )"""
            ),
            (
                "Przekroczenie limitu czasu w StayFree (>24h)",
                "SELECT count(*) FROM fact_stayfree WHERE usage_minutes > 1440"
            ),
            (
                "Sieroty w kalendarzu (brakujące daty w dim_calendar)",
                """SELECT count(*) FROM fact_stayfree 
                   WHERE CAST(date AS DATE) NOT IN (SELECT date FROM dim_calendar)"""
            )
        ]

        failed_any = False

        for test_name, query in checks:
            result = con.execute(query).fetchone()[0]
            
            if result == 0:
                logger.info(f"✅ PASS: {test_name}")
            else:
                logger.error(f"❌ FAIL: {test_name} - Znaleziono {result} błędnych rekordów!")
                failed_any = True

        if failed_any:
            logger.warning("Audyt zakończony: Wykryto błędy w danych.")
            sys.exit(1)  # Informujemy runner, że testy nie przeszły
        else:
            logger.info("Audyt zakończony sukcesem: Dane są czyste.")
            sys.exit(0)

    except Exception as e:
        logger.error(f"KRYTYCZNY BŁĄD podczas testów DQ: {str(e)}", exc_info=True)
        sys.exit(1)
        
    finally:
        if con:
            con.close()
            logger.info("Połączenie z DuckDB zostało zamknięte.")

if __name__ == "__main__":
    run_dq_checks()