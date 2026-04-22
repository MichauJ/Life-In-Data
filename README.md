🚀 Project Life-In-Data: Personal ETL Pipeline
Cel projektu: Budowa zautomatyzowanego ekosystemu analitycznego integrującego dane z aplikacji mobilnych (Habits, StayFree) w celu analizy korelacji między nawykami a cyfrowym dobrostanem.

🏗️ Architektura i Wybory Technologiczne
1. Konteneryzacja i Środowisko (Docker & WSL2)
Rozwiązanie: Cały proces ETL został zamknięty w kontenerze Docker działającym na WSL2.

Uzasadnienie: Zapewnienie izolacji środowiska (ta sama wersja bibliotek u każdego dewelopera) oraz bezpieczeństwa. Uruchamianie procesów jako dedykowany użytkownik Linuxowy (uid 1000) eliminuje problemy z uprawnieniami na styku Windows-Linux i odzwierciedla standardy produkcyjne (unikanie konta root).

2. Orkiestracja Danych (DVC - Data Version Control)
Rozwiązanie: Implementacja dvc.yaml do definiowania etapów (Stages) rurociągu.

Uzasadnienie: DVC wprowadza reprodukowalność. Pozwala śledzić, który skrypt wygenerował dany plik CSV. Dodatkowo umożliwia wersjonowanie dużych zbiorów danych bez "zapychania" Gita, co jest kluczowe w projektach Big Data.

3. Warstwa Składowania (DuckDB)
Rozwiązanie: Wykorzystanie DuckDB jako docelowego magazynu danych (OLAP).

Uzasadnienie: DuckDB jest niezwykle szybką, kolumnową bazą danych typu in-process. Idealnie nadaje się do analityki lokalnej, oferując wydajność zbliżoną do rozwiązań chmurowych (jak BigQuery) bez konieczności stawiania ciężkiego serwera bazy danych.

🛠️ Implementacja ETL (Extract, Transform, Load)
Etap Ingestii (Raw Layer)
Habits (SQLite): Skrypt automatycznie lokalizuje najnowszy backup bazy .db z Dysku Google, wykonuje zapytania SQL i eksportuje dane do formatu CSV.

StayFree (Excel): Implementacja zaawansowanego czyszczenia danych:

Unpivot (Melt): Zamiana szerokiego formatu Excela (daty w kolumnach) na format długi (tidy data).

Regex Parsing: Autorski parser czasu zamieniający stringi (np. 1h 20m) na wartości numeryczne (minuty).

Data Cleaning: Redukcja objętości danych o ok. 80-90% poprzez filtrowanie nieistotnych wpisów.

Etap Transformacji (Mart Layer)
Modelowanie: Budowa modelu w architekturze Star Schema (Schemat Gwiazdy).

Komponenty:

fact_stayfree & fact_habits: Tabele faktów z przeliczonymi miarami.

dim_calendar: Dynamicznie generowany wymiar czasu (kalendarz), zapewniający ciągłość analizy niezależnie od braków w danych źródłowych.

📈 Standardy Inżynierskie (The "Pro" Touch)
Centralna Konfiguracja (YAML & .env)
Zamiast "hardkodowania" ścieżek, projekt wykorzystuje system settings.yaml oraz zmienne środowiskowe .env.

Zaleta: Projekt jest przenośny. Zmiana ścieżki do Dysku Google wymaga edycji jednego pliku tekstowego, a nie całego kodu źródłowego.

Zaawansowane Logowanie (Observability)
Wprowadzono dedykowany moduł logger_setup.py.

Funkcja: Każde uruchomienie ETL generuje logi do konsoli oraz pliku data/pipeline.log.

Uzasadnienie: Rejestrowanie statystyk (np. liczba wierszy, czas trwania) pozwala na szybkie debugowanie i monitorowanie "zdrowia" danych bez zaglądania do bazy.

💡 Rozwiązywanie Problemów (Problem Solving)
W trakcie projektu zidentyfikowano i rozwiązano wąskie gardło wydajnościowe:

Wyzwanie: Bardzo wolne haszowanie plików przez DVC na montowanych dyskach sieciowych (GDrive przez WSL2).
Rozwiązanie: Zastosowano strategię hybrydową – użycie Pythona do szybkiej ingestii oraz optymalizację cache'u DVC (symlinks), co pozwoliło zachować logikę rurociągu przy ograniczeniach infrastrukturalnych.