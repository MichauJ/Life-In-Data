from src.ingestion.habits import process_habits
from src.ingestion.stayfree import process_stayfree
from src.transformation.build_mart import transform_data

def main():
    print("=== LIFE-IN-DATA: URUCHAMIANIE PIPELINE'U ===")
    
    # Krok: Habits
    try:
    # 1. Pobierz i wyczyść Nawyki
        process_habits()
        
        # 2. Pobierz i wyczyść StayFree
        process_stayfree()
        
        # 3. Połącz dane w DuckDB
        transform_data()
    except Exception as e:
        print(f"Błąd podczas przetwarzania: {e}")

    print("=== PROCES ZAKOŃCZONY ===")

if __name__ == "__main__":
    main()