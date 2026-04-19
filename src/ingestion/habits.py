import os
import sqlite3
import pandas as pd

def get_latest_habits_backup():
    backup_dir = "/app/gdrive_raw/Dokumenty/Me, Myself & I/Nawyki backup"
    if not os.path.exists(backup_dir):
        print(f"BŁĄD: Nie znaleziono folderu: {backup_dir}")
        return None

    # Pobieramy pełne ścieżki do wszystkich plików .db
    files = [os.path.join(backup_dir, f) for f in os.listdir(backup_dir) if f.endswith('.db')]
    
    if not files:
        print("Brak plików backupu Habits w folderze.")
        return None

    # KLUCZOWA ZMIANA: Wybieramy plik, który był ostatnio edytowany/zapisany na dysku
    latest_file = max(files, key=os.path.getmtime)
    return latest_file

def process_habits():
    path = get_latest_habits_backup()
    if not path:
        return

    # Wyświetlamy informację w konsoli (widoczną w docker compose up)
    print(f"\n--- 📂 ETAP HABITS ---")
    print(f"WYKORZYSTANY PLIK: {os.path.basename(path)}")
    print(f"----------------------")

    conn = sqlite3.connect(path)
    
    # Zapytanie bez zbędnych kolumn źródłowych
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
        
        # Zapisujemy czyste dane bez kolumny o źródle
        os.makedirs("data/raw/habits", exist_ok=True)
        df.to_csv("data/raw/habits/habits_history.csv", index=False)
        print(f"Sukces! Wyekstrahowano {len(df)} rekordów.")
        
    finally:
        conn.close()

if __name__ == "__main__":
    process_habits()