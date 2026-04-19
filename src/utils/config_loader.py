# src/utils/config_loader.py
import yaml
import os

def load_config():
    # Pobieramy ścieżkę do pliku config względem tego skryptu
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    config_path = os.path.join(base_dir, 'config', 'settings.yaml')
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    return config

# Inicjalizacja konfiguracji (singleton)
cfg = load_config()