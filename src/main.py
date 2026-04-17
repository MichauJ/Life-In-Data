import os

def list_gdrive_files():
    print("--- Life-In-Data: Inicjalizacja silnika ---")
    path = "/app/gdrive_raw"
    
    if os.path.exists(path):
        print(f"Sukces! Widzę Twój Google Drive pod ścieżką: {path}")
        files = os.listdir(path)
        print(f"Znaleziono {len(files)} obiektów w folderze głównym GDrive:")
        for f in files[:10]:  # Pokaż tylko pierwsze 10 dla porządku
            print(f" - {f}")
    else:
        print(f"BŁĄD: Nie znaleziono ścieżki {path}. Sprawdź montowanie dysku G: w WSL.")

if __name__ == "__main__":
    list_gdrive_files()