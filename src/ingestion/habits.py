import os
import sqlite3
import pandas as pd
from src.utils.config_loader import cfg
from src.utils.logger_setup import setup_logger

# Inicjalizacja profesjonalnego loggera
logger = setup_logger("ingest-habits")

def get_latest_habits_backup():
    """Wyszukuje najnowszy plik bazy danych .db w katalogu backupu."""
    backup_dir = cfg['paths']['habits_backup_dir']
    
    if not os.path.exists(backup_dir):
        logger.error(f"Katalog backupu nie istnieje: {backup_dir}")
        return None

    # Pobieramy pliki i wybieramy najnowszy po dacie modyfikacji
    files = [os.path.join(backup_dir, f) for f in os.listdir(backup_dir) if f.endswith('.db')]
    
    if not files:
        logger.warning(f"Nie znaleziono żadnych plików .db w katalogu: {backup_dir}")
        return None

    latest_file = max(files, key=os.path.getmtime)
    logger.info(f"Wybrano najnowszy backup: {os.path.basename(latest_file)}")
    return latest_file

def process_habits():
    """Główna funkcja przetwarzająca dane z Habits."""
    path = get_latest_habits_backup()
    if not path:
        logger.error("Przerwanie procesu: Nie odnaleziono pliku źródłowego.")
        return

    logger.info("Rozpoczynam ekstrakcję danych z bazy SQLite...")
    conn = sqlite3.connect(path)
    
    # Zapytanie wyciągające typy i wartości
    query = """
    SELECT 
        h.name AS habit_name,
        date(r.timestamp / 1000, 'unixepoch') AS date,
        h.type AS habit_type,
        h.unit AS habit_unit,
        r.value AS habit_value
    FROM habits h
    JOIN repetitions r ON h.id = r.habit
    WHERE h.archived = 0
    """
    
    try:
        df = pd.read_sql_query(query, conn)
        
        # Pobieramy ścieżkę zapisu z konfiguracji
        output_path = cfg['paths']['habits_raw_csv']
        
        # Tworzymy folder, jeśli nie istnieje
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Zapis do CSV
        df.to_csv(output_path, index=False)
        logger.info(f"Sukces! Wyekstrahowano {len(df)} rekordów do: {output_path}")
        
    except Exception as e:
        # exc_info=True sprawi, że w logach pojawi się pełny traceback błędu
        logger.error(f"Błąd podczas odczytu bazy Habits: {str(e)}", exc_info=True)
    finally:
        conn.close()
        logger.info("Połączenie z bazą SQLite zostało zamknięte.")

if __name__ == "__main__":
    process_habits()