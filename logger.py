import logging
from concurrent_log_handler import ConcurrentRotatingFileHandler # <-- NEW IMPORT
from pythonjsonlogger import jsonlogger
import os

LOG_BASE_DIR = 'logs'
os.makedirs(os.path.join(LOG_BASE_DIR, 'modules'), exist_ok=True)
os.makedirs(os.path.join(LOG_BASE_DIR, 'levels'), exist_ok=True)

def create_handler(filename, level):
    """Helper to create a multiprocess-safe rotating JSON handler."""
    path = os.path.join(LOG_BASE_DIR, filename)
    
    # --- ENTERPRISE FIX: Replaced standard handler with Concurrent handler ---
    # Max size 2.5 MB per file, keep 1 backup -> Total 5 MB max
    handler = ConcurrentRotatingFileHandler(path, maxBytes=int(2.5*1024*1024), backupCount=1)
    
    handler.setLevel(level)
    
    format_str = '%(asctime)s %(levelname)s %(name)s %(message)s'
    formatter = jsonlogger.JsonFormatter(format_str)
    handler.setFormatter(formatter)
    return handler



class LevelFilter(logging.Filter):
    """Custom filter to allow ONLY one specific level."""
    def __init__(self, level):
        self.level = level
    def filter(self, record):
        return record.levelno == self.level

def get_enterprise_logger(module_name):
    logger = logging.getLogger(module_name)
    if logger.hasHandlers():
        return logger
    
    # Only log WARNING, ERROR, and CRITICAL for production
    logger.setLevel(logging.WARNING)

    # --- 1. ROUTING BY MODULE (Where it came from) ---
    module_file = f"modules/{module_name.lower()}.json.log"
    logger.addHandler(create_handler(module_file, logging.WARNING))

    # --- 2. ROUTING BY LEVEL (Severity) ---
    # ERROR FILE (Catches ERROR and CRITICAL)
    logger.addHandler(create_handler("levels/errors.json.log", logging.ERROR))
    
    # WARNING FILE (ONLY Warnings)
    warn_handler = create_handler("levels/warnings.json.log", logging.WARNING)
    warn_handler.addFilter(LevelFilter(logging.WARNING)) # Strict filter
    logger.addHandler(warn_handler)

    # INFO FILE (ONLY Info)
    info_handler = create_handler("levels/info.json.log", logging.INFO)
    info_handler.addFilter(LevelFilter(logging.INFO)) # Strict filter
    logger.addHandler(info_handler)

    return logger
