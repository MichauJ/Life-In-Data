import os
import pandas as pd
import re
from src.utils.config_loader import cfg
from src.utils.logger_setup import setup_logger
import sys
# Inicjalizacja loggera
logger = setup_logger("ingest-stayfree")

def parse_stayfree_duration(duration_str):
    """Zamienia format czasu (np. '1h 20m 5s') na sumaryczną liczbę minut."""
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
    """Konwertuje polskie daty tekstowe lub obiekty datetime na format ISO."""
    if pd.isna(date_str):
        return None
        
    # Jeśli Excel już wczytał to jako datę (obiekt datetime)
    if not isinstance(date_str, str):
        try:
            return date_str.strftime('%Y-%m-%d')
        except:
            return str(date_str)

    miesiace = {
        'stycznia': '01', 'lutego': '02', 'marca': '03', 'kwietnia': '04',
        'maja': '05', 'czerwca': '06', 'lipca': '07', 'sierpnia': '08',
        'września': '09', 'października': '10', 'listopada': '11', 'grudnia': '12'
    }
    
    try:
        # Usuwamy zbędne białe znaki i normalizujemy
        clean_str = re.sub(r'\s+', ' ', str(date_str)).strip()
        parts = clean_str.split()
        
        if len(parts) < 3:
            return clean_str
            
        day = parts[0].zfill(2)
        month = miesiace.get(parts[1].lower(), '01')
        # Usuwamy ewentualne ".0" lub ".1" z roku
        year = parts[2].split('.')[0]
        
        return f"{year}-{month}-{day}"
    except Exception:
        return str(date_str)

def process_stayfree():
    """Główny proces ETL dla danych StayFree."""
    # Pobieramy ścieżki z centralnej konfiguracji
    source_dir = cfg['paths']['stayfree_source_dir']
    output_path = cfg['paths']['stayfree_raw_csv']
    
    logger.info(f"Rozpoczynam proces StayFree. Szukam plików w: {source_dir}")

    if not os.path.exists(source_dir):
        logger.error(f"Katalog źródłowy StayFree nie istnieje: {source_dir}")
        return

    all_files = os.listdir(source_dir)
    stayfree_files = [os.path.join(source_dir, f) for f in all_files if f.startswith("StayFree Export")]
    
    if not stayfree_files:
        logger.warning("Brak plików eksportu StayFree w katalogu źródłowym.")
        return

    latest_file = max(stayfree_files, key=os.path.getmtime)
    logger.info(f"Wybrano najnowszy plik: {os.path.basename(latest_file)}")

    try:
        # 1. Wczytujemy Excel (używamy xlrd dla plików .xls)
        logger.info("Wczytywanie arkusza Excel...")
        df = pd.read_excel(latest_file, engine='xlrd')
        
        # 2. Naprawa nazw kolumn
        df.rename(columns={df.columns[0]: 'aplikacja', 'Urządzenie': 'urządzenie'}, inplace=True)
        
        if 'Zużycie łącznie' in df.columns:
            df.drop(columns=['Zużycie łącznie'], inplace=True)

        # 3. Melt (Unpivot)
        logger.info("Transformacja formatu (melt) i czyszczenie danych...")
        df_long = df.melt(
            id_vars=['aplikacja', 'urządzenie'], 
            var_name='raw_date', 
            value_name='duration_raw'
        )

        # 4. Konwersje
        df_long['date'] = df_long['raw_date'].apply(parse_polish_date)
        df_long['usage_minutes'] = df_long['duration_raw'].apply(parse_stayfree_duration)

        # 5. Odchudzanie danych (usuwamy wpisy 0m)
        df_final = df_long[df_long['usage_minutes'] > 0].copy()
        df_final = df_final[['date', 'aplikacja', 'urządzenie', 'usage_minutes']]

        invalid_dates = df_final[~df_final['date'].str.contains(r'^\d{4}-\d{2}-\d{2}$', na=False)]
        
        if not invalid_dates.empty:
            sample_bad = invalid_dates['date'].iloc[0]
            logger.error(f"BŁĄD: Wykryto niepoprawne formaty dat po konwersji (np. '{sample_bad}').")
            logger.error("Proces przerwany, aby zapobiec zanieczyszczeniu bazy.")
            sys.exit(1) # Zatrzymujemy pipeline natychmiast!

        # 6. Zapis
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df_final.to_csv(output_path, index=False, encoding='utf-8')
        
        logger.info(f"Sukces! Dane zapisane w: {output_path}")
        logger.info(f"Rozmiar po optymalizacji: {len(df_final)} wierszy (zredukowano z ok. {len(df_long)})")

    except Exception as e:
        logger.error(f"Błąd krytyczny podczas przetwarzania StayFree: {str(e)}", exc_info=True)
        sys.exit(1)
if __name__ == "__main__":
    process_stayfree()