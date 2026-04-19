import os
import sqlite3
import pandas as pd
from src.utils.config_loader import cfg

def get_latest_habits_backup():
    # Pobieramy ścieżkę z YAML przez loader
    backup_dir = cfg['paths']['habits_backup_dir']
    
    if not os.path.exists(backup_dir):
        print(f"BŁĄD: Folder {backup_dir} nie istnieje.")
        return None

    # Pobieramy pliki i wybieramy najnowszy po dacie modyfikacji
    files = [os.path.join(backup_dir, f) for f in os.listdir(backup_dir) if f.endswith('.db')]
    
    if not files:
        print("Brak plików backupu Habits.")
        return None

    return max(files, key=os.path.getmtime)

def process_habits():
    path = get_latest_habits_backup()
    if not path:
        return

    print(f"\n--- 📂 ETAP HABITS ---")
    print(f"WYKORZYSTANY PLIK: {os.path.basename(path)}")

    conn = sqlite3.connect(path)
    
    # Zapytanie wyciągające typy i wartości (zgodnie z nową logiką)
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
        print(f"Sukces! Wyekstrahowano {len(df)} rekordów do {output_path}")
        
    except Exception as e:
        print(f"Błąd podczas odczytu bazy Habits: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    process_habits()