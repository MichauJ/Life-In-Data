import os
import sqlite3
import pandas as pd

def get_latest_habits_backup():
    backup_dir = "/app/gdrive_raw/Dokumenty/Me, Myself & I/Nawyki backup"
    if not os.path.exists(backup_dir):
        return None
    files = [f for f in os.listdir(backup_dir) if f.endswith('.db')]
    if not files:
        return None
    latest_file = sorted(files)[-1]
    return os.path.join(backup_dir, latest_file)

def process_habits():
    backup_path = get_latest_habits_backup()
    if not backup_path:
        print("Brak plików backupu.")
        return

    print(f"--- Przetwarzanie backupu Habits: {os.path.basename(backup_path)} ---")
    
    conn = sqlite3.connect(backup_path)
    
    query = """
    SELECT 
        h.name AS habit_name,
        date(r.timestamp / 1000, 'unixepoch') AS date,
        1 AS completed
    FROM habits h
    JOIN repetitions r ON h.id = r.habit
    WHERE h.archived = 0
    """
    
    try:
        df = pd.read_sql_query(query, conn)
        os.makedirs("data/raw/habits", exist_ok=True)
        df.to_csv("data/raw/habits/habits_history.csv", index=False)
        print(f"Sukces! Wyekstrahowano {len(df)} rekordów.")
    finally:
        conn.close()

if __name__ == "__main__":
    process_habits()