personal-analytics/
├── data/                 <-- Ten folder będzie pod kontrolą DVC
│   ├── raw/              <-- Nienaruszone pliki (CSV, .db, JSON z API)
│   │   ├── habits/
│   │   ├── stayfree/
│   │   ├── finance/
│   │   └── nextdns/
│   ├── processed/        <-- Dane po wstępnym czyszczeniu (format parquet)
│   └── final/            <-- Wynik końcowy (np. plik database.duckdb)
├── docker/               <-- Konfiguracja kontenerów
│   ├── Dockerfile
│   └── docker-compose.yml
├── src/                  <-- Kod źródłowy w Pythonie
│   ├── ingestion/        <-- Skrypty pobierające (API, GSheets)
│   ├── transformation/   <-- Logika łączenia danych (DuckDB/Polars)
│   └── utils/            <-- Funkcje pomocnicze (logowanie, połączenia)
├── tests/                <-- (Opcjonalnie) testy sprawdzające jakość danych
├── .dvc/                 <-- Konfiguracja DVC (tworzona automatycznie)
├── .gitignore            <-- Pliki pomijane przez Git (np. .env, dane)
├── .env                  <-- Twoje klucze API i hasła (ukryte!)
├── requirements.txt      <-- Lista bibliotek Pythona
└── main.py               <-- Główny skrypt uruchamiający cały proces