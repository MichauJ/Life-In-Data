import duckdb
import os

def transform_data():
    print("--- Rozpoczynam transformację w DuckDB ---")
    
    db_path = "data/final/life_in_data.duckdb"
    os.makedirs("data/final", exist_ok=True)
    
    # Łączymy się z bazą (plik powstanie, jeśli go nie ma)
    con = duckdb.connect(db_path)
    
    try:
        # 1. Wczytujemy StayFree i sumujemy czas per dzień
        # DuckDB potrafi czytać CSV bezpośrednio w zapytaniu SQL!
        print("Agregowanie danych StayFree...")
        con.execute("""
            CREATE OR REPLACE TABLE daily_usage AS
            SELECT 
                date, 
                SUM(usage_minutes) as total_screen_time_min,
                COUNT(DISTINCT aplikacja) as apps_count
            FROM read_csv_auto('data/raw/stayfree/stayfree_usage.csv')
            GROUP BY date
        """)
        
        # 2. Wczytujemy Habits
        print("Łączenie z nawykami...")
        con.execute("""
            CREATE OR REPLACE TABLE habits AS
            SELECT * FROM read_csv_auto('data/raw/habits/habits_history.csv')
        """)
        
        # 3. Tworzymy MASTER TABLE
        # Używamy LEFT JOIN, aby mieć wiersz nawet jeśli danego dnia nie było danych z telefonu
        con.execute("""
            CREATE OR REPLACE TABLE master_table AS
            SELECT 
                h.date,
                h.habit_name,
                h.completed,
                COALESCE(s.total_screen_time_min, 0) as screen_time_min,
                COALESCE(s.apps_count, 0) as apps_count
            FROM habits h
            LEFT JOIN daily_usage s ON h.date = s.date
            ORDER BY h.date DESC
        """)
        
        # Sprawdzenie wyniku
        rows = con.execute("SELECT COUNT(*) FROM master_table").fetchone()[0]
        print(f"Sukces! Master Table gotowa. Liczba wierszy: {rows}")
        
    finally:
        con.close()

if __name__ == "__main__":
    transform_data()