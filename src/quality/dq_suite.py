import duckdb
from src.utils.config_loader import cfg
from src.utils.logger_setup import setup_logger

logger = setup_logger("data-quality")

def run_dq_checks():
    db_path = cfg['paths']['final_db']
    con = duckdb.connect(db_path)
    
    logger.info("--- Rozpoczynam audyt jakości danych ---")
    
    # Definicja testów w formie: (Nazwa testu, Zapytanie SQL zwracające błędy)
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
               WHERE date NOT IN (SELECT date FROM dim_calendar)"""
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

    con.close()
    
    if failed_any:
        logger.warning("Audyt zakończony: Wykryto błędy w danych. Sprawdź logi.")
    else:
        logger.info("Audyt zakończony sukcesem: Dane są gotowe do analizy.")

if __name__ == "__main__":
    run_dq_checks()