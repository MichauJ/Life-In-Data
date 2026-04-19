import os
import pandas as pd
import re

def parse_stayfree_duration(duration_str):
    """Zamienia '1h 20m 5s' lub '15m' na sumaryczną liczbę minut (float)."""
    if pd.isna(duration_str) or duration_str == "0s" or duration_str == 0:
        return 0.0
    
    duration_str = str(duration_str).lower()
    hours = re.search(r'(\d+)h', duration_str)
    minutes = re.search(r'(\d+)m', duration_str)
    seconds = re.search(r'(\d+)s', duration_str)
    
    total_minutes = 0.0
    if hours:
        total_minutes += int(hours.group(1)) * 60
    if minutes:
        total_minutes += int(minutes.group(1))
    if seconds:
        total_minutes += int(seconds.group(1)) / 60
        
    return round(total_minutes, 2)

def parse_polish_date(date_str):
    """Zamienia '1 stycznia 2026' na '2026-01-01'."""
    miesiace = {
        'stycznia': '01', 'lutego': '02', 'marca': '03', 'kwietnia': '04',
        'maja': '05', 'czerwca': '06', 'lipca': '07', 'sierpnia': '08',
        'września': '09', 'października': '10', 'listopada': '11', 'grudnia': '12'
    }
    try:
        parts = date_str.split()
        day = parts[0].zfill(2)
        month = miesiace.get(parts[1].lower(), '01')
        year = parts[2]
        return f"{year}-{month}-{day}"
    except:
        return date_str

def process_stayfree():
    source_dir = "/app/gdrive_raw/Dokumenty/Me, Myself & I/StayFree statystyki"
    target_dir = "data/raw/stayfree"
    
    all_files = os.listdir(source_dir)
    stayfree_files = [os.path.join(source_dir, f) for f in all_files if f.startswith("StayFree Export")]
    
    if not stayfree_files:
        print("Brak plików StayFree.")
        return

    latest_file = max(stayfree_files, key=os.path.getmtime)
    print(f"--- Przetwarzanie StayFree (Format Szeroki): {os.path.basename(latest_file)} ---")

    try:
        # 1. Wczytujemy Excel
        df = pd.read_excel(latest_file, engine='xlrd')
        
        # 2. Naprawiamy nazwy głównych kolumn
        # Pierwsza kolumna nie ma nazwy (Unnamed: 0), a tam są aplikacje
        df.rename(columns={df.columns[0]: 'aplikacja', 'Urządzenie': 'urządzenie'}, inplace=True)
        
        # 3. Usuwamy kolumnę podsumowującą, jeśli istnieje
        if 'Zużycie łącznie' in df.columns:
            df.drop(columns=['Zużycie łącznie'], inplace=True)

        # 4. UNPIVOT (Melt) - Zamieniamy daty-kolumny na wiersze
        # id_vars to kolumny, które zostają. Reszta (daty) zostanie "stopiona" w jedną kolumnę.
        df_long = df.melt(
            id_vars=['aplikacja', 'urządzenie'], 
            var_name='raw_date', 
            value_name='duration_raw'
        )

        print(f"Format po melt: {df_long.shape[0]} wierszy. Rozpoczynam czyszczenie wartości...")

        # 5. Konwersja dat na format ISO (YYYY-MM-DD)
        df_long['date'] = df_long['raw_date'].apply(parse_polish_date)

        # 6. Konwersja czasu na minuty
        df_long['usage_minutes'] = df_long['duration_raw'].apply(parse_stayfree_duration)

        # 7. Usuwamy wiersze, gdzie czas to 0 (oszczędność miejsca, z 260k zrobi się pewnie 20k)
        df_final = df_long[df_long['usage_minutes'] > 0].copy()

        # Usuwamy zbędne kolumny techniczne
        df_final = df_final[['date', 'aplikacja', 'urządzenie', 'usage_minutes']]

        # 8. Zapis do CSV
        os.makedirs(target_dir, exist_ok=True)
        output_path = os.path.join(target_dir, "stayfree_usage.csv")
        df_final.to_csv(output_path, index=False, encoding='utf-8')
        
        print(f"Sukces! Dane 'odchudzone' i zapisane w: {output_path}")
        print(f"Liczba istotnych wpisów: {len(df_final)}")

    except Exception as e:
        print(f"Błąd krytyczny StayFree: {e}")

if __name__ == "__main__":
    process_stayfree()