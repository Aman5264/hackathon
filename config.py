import os
from dotenv import load_dotenv
from logger import get_enterprise_logger
from core.database import get_system_setting

log = get_enterprise_logger("ConfigValidator")

load_dotenv()

class Config:
    """The Single Source of Truth for all application settings."""
    
    SECRET_KEY = os.environ.get('FLASK_SECRET_KEY', 'default-dev-key')
    # ADMIN_PASSWORD = os.environ.get('CLINIC_ADMIN_PASSWORD', 'admin1')
    DICOM_PORT = int(get_system_setting('DICOM_PORT', 11114))
    INCOMING_FOLDER = os.environ.get('INCOMING_FOLDER', 'data/incoming')
    PROCESSING_FOLDER = os.environ.get('PROCESSING_FOLDER', 'data/processing')
    OUTGOING_FOLDER = os.environ.get('OUTGOING_FOLDER', 'data/outgoing')
    ARCHIVE_FOLDER = os.environ.get('ARCHIVE_FOLDER', 'data/archive')
    MAX_CONNECTIONS = int(get_system_setting('MAX_CONCURRENT_CONNECTIONS', 10))
    BACKUP_FOLDER =  os.environ.get('BACKUP_FOLDER', 'data/backup')
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URI', 'sqlite:///dicom_data.db')
    DICOM_AE_TITLE =  get_system_setting('DICOM_AE_TITLE', 'RADNOVA_STORE')
    HOST =  get_system_setting('HOST', '0.0.0.0')
    RETENTION_DAYS = int(os.environ.get('RETENTION_DAYS', 30))
    MIN_DISK_FREE_PERCENT = 5.0
    WEB_UI_PORT = int(os.environ.get('WEB_UI_PORT', 5000))

    # API_UPLOAD_URL = os.getenv('API_UPLOAD_URL', 'http://127.0.0.1:5001/api/upload')
    
    # --- CLOUD SFTP CONFIGURATION (PORT 22) ---
    SFTP_HOST = os.getenv('SFTP_HOST', 'ftpicare.anikrafoundation.com')
   # Accepts a list of ports like "26,22,27,28". 
    # Python splits them into a list of integers: [26, 22, 27, 28]
    raw_ports = os.getenv('SFTP_PORTS', '26,21,28')
    SFTP_PORTS = [int(p.strip()) for p in raw_ports.split(',')]
    SFTP_USER = os.getenv('SFTP_USER', 'ftp1')
    # The Bearer token for secure authentication
    API_SECRET_KEY = os.getenv('API_SECRET_KEY', 'ftp1')
    
    # --- NETWORK RESILIENCE CONFIGURATION ---
    # Max time (in seconds) to wait for the API to respond before severing the connection
    UPLOAD_TIMEOUT = int(os.getenv('UPLOAD_TIMEOUT', 120)) 
    
    # Maximum number of times to retry a failed upload
    MAX_UPLOAD_RETRIES = int(os.getenv('MAX_UPLOAD_RETRIES', 4))
   
    AUTH_API_URL =  get_system_setting('AUTH_API_URL')
    DESCRIPTION_API_URL = get_system_setting('DESCRIPTION_API_URL', 'https://icare.anikrafoundation.com:8443/weasisintegratorwf/lookupmasterservice/getdescription/')
    Desc_S_date = "2000-01-01"
    UPLOAD_CHUNK_SIZE_LEGACY = 8192
    UPLOAD_CHUNK_SIZE_MODERN = 65536
    ADMIN_PASSWORD = os.environ.get('ADMIN_PASSWORD', 'admin123')
    
    
    # Whenever the uploader initializes or checks its config:
    TUS_API_URL = get_system_setting('TUS_API_URL', 'http://127.0.0.1:1080/files/')
    TUS_API_KEY = get_system_setting('TUS_API_KEY', '')

    # --- THREADING & CONCURRENCY LIMITS ---
    from systemProfile import SystemProfile
    WAITRESS_THREADS = int(os.environ.get('WAITRESS_THREADS', 16))
    COMPRESSION_WORKERS = int(get_system_setting('COMPRESSION_WORKERS', SystemProfile.get_optimal_threads('CPU')))
    UPLOAD_WORKERS = int(get_system_setting('UL_WORKERS', SystemProfile.get_optimal_threads('IO')))
    ZIP_MODE = get_system_setting('ZIP_MODE', 'STORED') # STORED or DEFLATED
    @classmethod
    def validate(cls):
        """The Bouncer: Runs once at startup to enforce security rules."""
        log.info("Validating system configuration on startup")
        
        # 1. SECURITY VALIDATION: Prevent weak passwords in production
        # weak_passwords = ['admin', 'password', '123456', 'root']
        # if cls.ADMIN_PASSWORD.lower() in weak_passwords:
        #     log.critical("Startup failed: Weak admin password detected in environment variables")
        #     raise ValueError(f"FATAL SECURITY ERROR: The password '{cls.ADMIN_PASSWORD}' is strictly forbidden. Please update your .env file!")
            
        # 2. TYPE VALIDATION: Ensure the port is valid
        if not (1024 <= cls.DICOM_PORT <= 65535):
            log.critical("Startup failed: Invalid DICOM port configured", extra={"configured_port": cls.DICOM_PORT})
            raise ValueError(f"CONFIG ERROR: DICOM_PORT ({cls.DICOM_PORT}) must be between 1024 and 65535!")
            
        # 3. DATA VALIDATION: Ensure critical paths aren't blank
        if not cls.INCOMING_FOLDER:
            log.critical("Startup failed: INCOMING_FOLDER path is empty")
            raise ValueError("CONFIG ERROR: INCOMING_FOLDER cannot be an empty string!")
            
        log.info("System configuration validation passed securely")
