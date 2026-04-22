import logging
import sys
from pathlib import Path

def setup_logger(name: str, log_file: str = "data/pipeline.log"):
    """Konfiguruje logger z wyjściem na konsolę i do pliku."""
    
    # Upewnij się, że folder na logi istnieje
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Format logów: Czas - NazwaModułu - Poziom - Wiadomość
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Handler 1: Konsola (dla 'docker logs')
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Handler 2: Plik (dla historii)
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger