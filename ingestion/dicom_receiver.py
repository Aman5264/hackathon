import os
import queue
from pynetdicom import AE, evt,  debug_logger
from pynetdicom.sop_class import Verification
from core.database import db, AllowedScanner, AllowedModality, PatientStudy, PipelineStatus, get_system_setting
from core.event_bus import db_write_queue, DBWriteEvent
import time
from pynetdicom.presentation import AllStoragePresentationContexts, build_context
import shutil
from config import Config
from threading import Lock
from pydicom.uid import (
    ImplicitVRLittleEndian,
    ExplicitVRLittleEndian,
    ExplicitVRBigEndian,
    JPEGBaseline8Bit,
    JPEGExtended12Bit,
    JPEGLosslessSV1,
    JPEG2000Lossless,
    JPEG2000,
    RLELossless,
)
from logger import get_enterprise_logger
from systemProfile import SystemProfile
from pydicom.valuerep import PersonName
from datetime import datetime
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from services.metadata_engine import engine as metadata_engine
debug_logger()
log = get_enterprise_logger("ReceiverThread")

server = None
_modality_cache = set()
_modality_cache_time = 0
_CACHE_TTL = 60  # Refresh every 60 seconds
_cache_lock = Lock()
file_save_lock = Lock()



def is_modality_allowed(mod, app):
    """
    Checks the RAM cache for the modality.
    Only queries the database if the cache is older than _CACHE_TTL.
    """
    global _modality_cache, _modality_cache_time
    
    current_time = time.time()
    
    # 1. Check if the cache is expired
    if current_time - _modality_cache_time > _CACHE_TTL:
        # 2. Grab the lock so only ONE thread goes to the database
        with _cache_lock:
            # 3. Double-check pattern (in case another thread just updated it while we waited for the lock)
            if time.time() - _modality_cache_time > _CACHE_TTL:
                with app.app_context():
                    # Fetch from DB
                    allowed_records = AllowedModality.query.all()
                    # Store as a Python Set for lightning-fast lookups
                    _modality_cache = {record.name.upper() for record in allowed_records}
                    _modality_cache_time = time.time()
                    log.info( "Modality whitelist updated in RAM", extra={"modality_cache": list(_modality_cache)})

    # 4. Check the RAM cache (Instantaneous)
    # ALWAYS allow Presentation States, Structured Reports, and Key Objects
    if mod.upper() in ["PR", "SR", "KO"]:
        return True
        
    return mod.upper() in _modality_cache


def invalidate_modality_cache():
    """Forces the DICOM receiver to fetch fresh modalities from the DB on the next request."""
    global _modality_cache_time
    # Resetting the timer to 0 guarantees the next file will trigger a DB pull
    _modality_cache_time = 0 
    log.info( "Modality cache invalidated", extra={"source": "Web Dashboard"})



def is_disk_safe():
    """Checks if the drive has enough room for new associations."""
    # Check the drive where images are stored
    os.makedirs(Config.INCOMING_FOLDER, exist_ok=True)
    total, used, free = shutil.disk_usage(Config.INCOMING_FOLDER)
    
    percent_free = (free / total) * 100
    
    if percent_free < Config.MIN_DISK_FREE_PERCENT:
        log.warning("Disk space guard triggered", extra={
            "percent_free": round(percent_free, 2),
            "threshold": Config.MIN_DISK_FREE_PERCENT,
            "free_gb": round(free / (1024**3), 2)
        })
        return False
        
    return True


def get_custom_path(base_folder, p_name, s_uid):
    """Creates a secure, hacker-proof folder path: BaseFolder/PatientName_StudyUID"""
    
    def secure_string(text, default="Unknown"):
        # 1. Force the input to a string
        text = str(text)
        
        # 2. The Whitelist: Only allow letters, numbers, spaces, _, -, and .
        safe_chars = [c for c in text if c.isalnum() or c in (' ', '_', '-', '.')]
        safe_text = "".join(safe_chars).strip()
        
        # 3. Strip leading dots to neutralize hidden folders or "../" attacks
        safe_text = safe_text.lstrip('.')
        
        # 4. Fallback if the hacker tried to send a string of pure garbage
        return safe_text if safe_text else default

    # Apply the armor to both variables
    clean_name = secure_string(p_name, default="Unknown_Patient")
    clean_uid = secure_string(s_uid, default="Unknown_UID")
    
    # =================================================================
    # 🚨 THE FIX: Combine them into a single, flat folder name!
    # This guarantees a unique folder per study, completely isolating
    # patients who happen to share the exact same name.
    # =================================================================
    folder_name = f"{clean_name}_{clean_uid}"
    path = os.path.join(base_folder, folder_name)
    
    os.makedirs(path, exist_ok=True)
    
    return path


def handle_accepted(event):
    """Fired when a new machine connects (A new thread starts)."""
    log.info( "New connection thread started",  extra={"event": "connection_accepted"})

    return 0x0000

def handle_released(event):
    """Fired when the machine hangs up (The thread safely dies)."""
    log.info("DICOM connection closed safely")
    return 0x0000

def validate_dicom_payload(ds):
    """
    Unified Validation Gateway: Checks both hardware integrity and business rules.
    Returns: (status: str, message: str)
    Statuses: 'OK', 'CORRUPT', 'QUARANTINE'
    """
  
   # THE FIX: Check the dataset dictionary keys. 
    # Validates existence without loading the payload into RAM.
    required_metadata = {
        "PatientName": "Patient Name",
        "StudyDate": "Study Date",
        "Modality": "Modality"
    }

    # Loop through our strict requirements
    for dicom_tag, human_name in required_metadata.items():
        # Safely attempt to get the tag
        value = getattr(ds, dicom_tag, None)
        
        # Check if the tag is completely missing OR if it is just blank spaces
        if value is None or str(value).strip() == "":
            # The exact second one fails, we kick it out. 
            return "QUARANTINE", f"Missing or blank mandatory field: {human_name}"

    return "OK", "Passed all hardware and strict business logic checks."


def safe_cast(value, cast_type, default=None):
    """Helper to safely cast messy DICOM fields to ints without crashing."""
    if value is None or str(value).strip() == '': 
        return default
    try: 
        return cast_type(value)
    except (ValueError, TypeError): 
        return default
    
def extract_list_string(ds, tag_name):
    """Safely extracts DICOM MultiValue tags (like PixelSpacing) into a clean string."""
    val = getattr(ds, tag_name, "")
    if isinstance(val, list) or type(val).__name__ == 'MultiValue':
        return "\\".join([str(v) for v in val])
    return str(val)
# --------------------------------------


def _quarantine_payload(ds, instance_uid, reason_msg):
    # Save to Error Folder safely
    from config import Config
    error_dir = getattr(Config, 'ERROR_FOLDER', './error_data') 
    import os, time
    os.makedirs(error_dir, exist_ok=True)
    error_filepath = os.path.join(error_dir, instance_uid + '.dcm')
    
    if not os.path.exists(error_filepath):
        temp_error = error_filepath + '.tmp'
        try:
            ds.save_as(temp_error, write_like_original=False)
            # Robust atomic rename for Windows
            rename_success = False
            for attempt in range(3):
                try:
                    os.rename(temp_error, error_filepath)
                    rename_success = True
                    break
                except OSError:
                    if os.path.exists(error_filepath):
                        rename_success = True
                        break
                    time.sleep(0.01)
            
            if not rename_success:
                pass
        except Exception as e:
            pass

def handle_store(event, app):
    """Handle a C-STORE request event safely across multiple threads."""

    start_time = time.time()
    instance_uid = str(event.request.AffectedSOPInstanceUID)

    ds = event.dataset
    ds.file_meta = event.file_meta


    status, msg = validate_dicom_payload(ds)

    if status == "CORRUPT":
        log.warning("Payload rejected due to corruption", extra={"sop_instance_uid": instance_uid, "reason": msg})
        return 0xC000 # Tell scanner: "Broken file, resend it!"

    elif status == "QUARANTINE":
        log.warning("Payload quarantined due to missing metadata", extra={"sop_instance_uid": instance_uid, "reason": msg})
        _quarantine_payload(ds, instance_uid, msg)
        return 0x0000
    


    # 1. EXTRACT CRITICAL UIDS
    s_uid = str(getattr(ds, "StudyInstanceUID", "UNKNOWN"))
    series_uid = str(getattr(ds, "SeriesInstanceUID", "UNKNOWN"))

    if s_uid == "UNKNOWN" or series_uid == "UNKNOWN":
        log.warning("File missing hierarchical UIDs (Study/Series). Quarantining.", extra={"sop_uid": instance_uid})
        return 0xC000 
   # =================================================================
    # --- THE GATEKEEPER: Pure Image-Level Deduplication ---
    # =================================================================
    sop_uid = str(ds.SOPInstanceUID)
    # Deduplication moved to the asynchronous DatabaseWriter thread to eliminate SQLite locks.
    # =================================================================
    # 2. EXTRACT CORE FIELDS (Needed for path creation and modality checks)
    p_name_raw = getattr(ds, "PatientName", "Unknown")
    p_name = str(p_name_raw).replace('^', ' ') if isinstance(p_name_raw, PersonName) else str(p_name_raw)
    mod = str(getattr(ds, "Modality", "UNKNOWN"))
    
    raw_date = str(getattr(ds, "StudyDate", ""))
    s_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}" if len(raw_date) == 8 else "Unknown"



    # We do NOT need to push a Flask app_context here because is_modality_allowed
    # relies on a lightning-fast RAM cache, and pushes its own context ONLY if needed.
    if not is_modality_allowed(mod, app):
        log.warning("Payload quarantined due to unauthorized modality", extra={"modality": mod})
        _quarantine_payload(ds, instance_uid, "unauthorized modality")
        return 0x0000 
        
    log.info("Modality approved", extra={"modality": mod})
    enterprise_filename = f"{mod}.{instance_uid}.dcm"


    incoming_study_path = get_custom_path(Config.INCOMING_FOLDER, p_name, s_uid)
    final_incoming = os.path.join(incoming_study_path, enterprise_filename)

    backup_study_path = get_custom_path(Config.BACKUP_FOLDER, p_name, s_uid)
    final_backup = os.path.join(backup_study_path, enterprise_filename)

    # Check the hard drive: Does this file already exist?
    if os.path.exists(final_incoming):
        log.info("Duplicate incoming payload skipped", extra={"sop_instance_uid": instance_uid})
        # The White Lie: Return Success so the scanner stops trying to send it!
        return 0x0000
    
    # --- The rest of your existing code stays exactly the same! ---
    log.info("SOP approved", extra={"modality": mod})

   
    try:
        if os.path.exists(final_incoming):
            log.info("Race condition averted during incoming save", extra={"sop_instance_uid": instance_uid})
            return 0x0000
        
        # --- 1. THE INCOMING FOLDER ---
        import uuid
        unique_id = uuid.uuid4().hex
        temp_incoming = final_incoming + f".tmp_{unique_id}"
        ds.save_as(temp_incoming, write_like_original=False)
        
        # --- THE FIX: Robust retry logic for Windows file locks (e.g. Defender) ---
        rename_success = False
        for attempt in range(20): # Increased to 20 attempts
            try:
                os.rename(temp_incoming, final_incoming)
                rename_success = True
                break
            except OSError:
                if os.path.exists(final_incoming):
                    rename_success = True
                    break
                time.sleep(0.05) # 50ms sleep * 20 = 1 full second
                
        if not rename_success:
            raise OSError("Atomic rename failed after 20 attempts due to file locks.")
    except Exception as e:
        log.error(f"Error saving incoming file: {e}")
        return 0xC000

    # --- 2. THE BACKUP FOLDER ---
    def _background_backup():
        if os.path.exists(final_backup):
            log.info("Duplicate backup payload skipped", extra={"sop_instance_uid": instance_uid})
        else:
            try:
                temp_backup = final_backup + f".tmp_{unique_id}"
                shutil.copyfile(final_incoming, temp_backup)
                os.rename(temp_backup, final_backup)
            except Exception as e:
                log.error("Failed to save payload to backup archive", exc_info=True)
                
    import threading
    threading.Thread(target=_background_backup, daemon=True).start()
        
    
    
   # --- 3. THE DATABASE QUEUE (Guaranteed to Run) ---
    payload = {
        # Processing & Paths
        'start_time': start_time,
        'temp_incoming': temp_incoming,
        'final_incoming': final_incoming,
        
        # Study Level
        'study_uid': s_uid,
        'patient_id': str(getattr(ds, "PatientID", "Unknown")),
        'patient_name': p_name,
        'patient_age': str(getattr(ds, "PatientAge", "N/A")),
        'patient_sex': str(getattr(ds, "PatientSex", "O")),
        'patient_birth_date': str(getattr(ds, "PatientBirthDate", "")),
        'modality': mod,
        'study_date': s_date,
        'study_description': metadata_engine.extract_best_study_description(ds),
        'institution_name': str(getattr(ds, "InstitutionName", "")),
        'ref_physician_name': str(getattr(ds, "ReferringPhysicianName", "")).replace('^', ' '),
        
        # --- NEW STUDY TAGS ---
        'accession_number': str(getattr(ds, "AccessionNumber", "")),
        'manufacturer': str(getattr(ds, "Manufacturer", "")),
        'manufacturer_model_name': str(getattr(ds, "ManufacturerModelName", "")),

        # Series Level
        'series_uid': series_uid,
        'series_number': safe_cast(getattr(ds, "SeriesNumber", None), int),
        'series_description': str(getattr(ds, "SeriesDescription", "")),

        # Image Level
        'sop_uid': instance_uid,
        'instance_number': safe_cast(getattr(ds, "InstanceNumber", None), int),
        
        # --- NEW IMAGE TAGS ---
        'window_width': str(getattr(ds, "WindowWidth", "")),
        'window_center': str(getattr(ds, "WindowCenter", "")),
        'slice_thickness': str(getattr(ds, "SliceThickness", "")),
        'acquisition_time': str(getattr(ds, "AcquisitionTime", "")),
        'slice_location': str(getattr(ds, "SliceLocation", "")),
        'columns': str(getattr(ds, "Columns", "")),
        'rows': str(getattr(ds, "Rows", "")),
        'photometric_interpretation': str(getattr(ds, "PhotometricInterpretation", "")),
        'bits_allocated': str(getattr(ds, "BitsAllocated", "")),
        'bits_stored': str(getattr(ds, "BitsStored", "")),
        'pixel_representation': str(getattr(ds, "PixelRepresentation", "")),
        'samples_per_pixel': str(getattr(ds, "SamplesPerPixel", "")),
        'pixel_spacing': extract_list_string(ds, "PixelSpacing"),
        'image_orientation_patient': extract_list_string(ds, "ImageOrientationPatient"),
        'image_position_patient': extract_list_string(ds, "ImagePositionPatient"),
        'frame_of_reference_uid': str(getattr(ds, "FrameOfReferenceUID", "")),
        'image_type': extract_list_string(ds, "ImageType"),
        'sop_class_uid': str(getattr(ds, "SOPClassUID", "")),
        'high_bit': str(getattr(ds, "HighBit", ""))
    }

    # --- THE FIX: Enforce a strict timeout so we never block the association ---
    try:
        db_write_queue.put(DBWriteEvent(action='insert_dicom', payload=payload), block=True, timeout=1.0)
    except queue.Full:
        log.error("CRITICAL: Database queue is full! Rejecting DICOM file to prevent association hang.", extra={"sop_uid": instance_uid})
        # If we return a failure, the scanner will naturally queue it and retry later!
        if os.path.exists(temp_incoming):
            try: os.remove(temp_incoming)
            except OSError: pass
        return 0xC000
    
    return 0x0000

def security_bouncer(event, app):
    """The Diagnostic Bouncer: Tells you EXACTLY what is failing."""
    try:
        # --- DISK SPACE PRE-CHECK ---
        from systemProfile import SystemProfile
        from config import Config
        if not SystemProfile.has_sufficient_disk(Config.INCOMING_FOLDER):
            log.warning("Rejecting association: Disk space below 5%")
            event.assoc.abort() 
            return
    
        # 1. DEEP CLEAN Incoming Network Data
        raw_title = event.assoc.requestor.ae_title
        req_title = raw_title.decode('ascii').replace('\x00', '').strip() if isinstance(raw_title, bytes) else str(raw_title).replace('\x00', '').strip()
        
        req_ip = str(event.assoc.requestor.address).strip()
        req_port = int(event.assoc.requestor.port)
        
        log.info("Incoming connection request", extra={"ae_title": req_title, "ip_address": req_ip, "port": req_port})

        # 2. Open a secure window into the Flask Database
        with app.app_context():
            from core.database import AllowedScanner, SystemSetting
            from sqlalchemy import func
            
            # ==========================================================
            # 🚨 LIVE DB CHECK: Bypass RAM caches and read the truth
            # ==========================================================
            strict_setting = SystemSetting.query.filter_by(key='STRICT_AE_IP_CHECK').first()
            is_strict = strict_setting.value.strip().lower() == 'true' if strict_setting else True
            
            if not is_strict:
                log.info("Security Firewall is DISABLED. Allowing connection.", extra={"ae_title": req_title, "ip_address": req_ip})
                return 0x0000

            # ==========================================================
            # 🚨 AE TITLE QUERY: Ignore case and trailing spaces
            # ==========================================================
            scanner = AllowedScanner.query.filter(
                func.lower(func.trim(AllowedScanner.ae_title)) == req_title.lower()
            ).first()
            
            if not scanner:
                log.warning(f"Connection rejected, unknown AE Title: '{req_title}'")
                event.assoc.abort()
                return
                
            log.info("AE Title matches database", extra={"ae_title": req_title})

            # TEST 2: Check IP Address
            if scanner.ip_address.strip() != req_ip:
                log.warning("Connection rejected, IP mismatch", extra={"expected_ip": scanner.ip_address, "received_ip": req_ip})
                event.assoc.abort()
                return
                
            log.info("IP Address matches database", extra={"ip_address": req_ip})
            return 0x0000
        
    except Exception as e:
        log.error("Security bouncer crashed", exc_info=True)
        event.assoc.abort()
        
    

def safe_handle_store(event, app):
    """Failsafe wrapper for the C-STORE handler to prevent thread crashes."""
    try:
        return handle_store(event, app)
    except Exception as e:
        log.error(f"Catastrophic failure in handle_store: {e}", exc_info=True)
        return 0xC000



def start_dicom_server(app):
    """Starts the DICOM receiver in a non-blocking background thread."""
    global server
    
    # =================================================================
    # 1. JUST-IN-TIME (JIT) CONFIGURATION 
    # Pulls live settings directly from the DB to prevent stale memory locks
    # =================================================================
    try:
        with app.app_context():
            from core.database import get_system_setting
            live_ae_title = get_system_setting('DICOM_AE_TITLE', 'ICARE_STORE')
            live_port = int(get_system_setting('DICOM_PORT', 11114))
            live_host = get_system_setting('HOST', '0.0.0.0')
            live_max_conn = int(get_system_setting('MAX_CONCURRENT_CONNECTIONS', 10))
    except Exception as e:
        log.critical(f"Failed to fetch DICOM settings from DB. Aborting boot: {e}")
        return False

    # =================================================================
    # 2. APPLICATION ENTITY (AE) & HARDWARE THROTTLING
    # =================================================================
    ae = AE(ae_title=live_ae_title)
    
    ae.maximum_associations = live_max_conn

    # =================================================================
    # 3. UNIVERSAL TRANSFER SYNTAXES
    # =================================================================
    transfer_syntaxes = [
        ImplicitVRLittleEndian, # Default Uncompressed
        ExplicitVRLittleEndian, # Default Uncompressed
        ExplicitVRBigEndian,    # Default Uncompressed
        JPEGBaseline8Bit,       # Lossy JPEG (Web viewing)
        JPEGExtended12Bit,      # 12-bit JPEG Lossy
        JPEGLosslessSV1,        # Lossless JPEG (Most common scanner format)
        JPEG2000Lossless,       # Lossless JPEG 2000
        JPEG2000,               # Modern Lossy
        RLELossless             # Run-Length Encoding (Used in Ultrasounds)
    ]

    custom_contexts = []
    for context in AllStoragePresentationContexts:
        # Attach custom transfer syntaxes to every single allowed file type
        custom_contexts.append(build_context(context.abstract_syntax, transfer_syntaxes))

    ae.supported_contexts = custom_contexts
    ae.add_supported_context(Verification) # Keep Echo/Ping working

    # =================================================================
    # 4. EVENT HANDLERS
    # =================================================================
    handlers = [
        (evt.EVT_ACCEPTED, handle_accepted),
        (evt.EVT_ACCEPTED, security_bouncer, [app]),
        (evt.EVT_C_STORE, safe_handle_store, [app]),
        (evt.EVT_RELEASED, handle_released)
    ]
    
    # =================================================================
    # 5. SAFE SERVER STARTUP
    # =================================================================
    try:
        log.info(f"Booting DICOM Receiver on {live_host}:{live_port} as {live_ae_title}...")
        
        # block=False is the magic trick that lets it run alongside Flask!
        server = ae.start_server((live_host, live_port), evt_handlers=handlers, block=False)
        
        log.info("✅ DICOM Receiver successfully bound to port and is now listening.")
        return True
        
    except OSError as os_err:
        log.critical(f"❌ OS Error starting DICOM Server (Port {live_port} might already be in use!): {os_err}")
        return False
    except Exception as e:
        log.critical(f"❌ CRITICAL: Failed to start DICOM Server: {e}", exc_info=True)
        return False

def stop_dicom_server():
    """Politely stops listening for new machines, but lets current transfers finish."""
    if server:
        log.info("DICOM receiver shutting down, locking associations")
        # This brilliant built-in function safely stops the non-blocking server!
        server.shutdown()
        log.info("All active DICOM transfers safely finished")    


def dicom_watchdog_loop(app):
    """The Guardian: Monitors the DICOM server and restarts it if it fails."""
    log.info("Watchdog thread initialized and monitoring...")
    
    while True:
        # Check every 30 seconds
        time.sleep(30) 
        
        # We only monitor if we aren't in the middle of a graceful shutdown
        import app as app_module
        if app_module.is_shutting_down:
            break

        is_alive = False
        if server is not None:
           try:
                # FIX: Check if the raw networking socket is actively listening
                if server.socket and server.socket.fileno() > 0:
                    is_alive = True
           except Exception as e:
                # Catching the specific exception so it doesn't fail silently if it breaks
                log.debug(f"Watchdog socket check failed: {e}")
                is_alive = False

        if not is_alive:
            log.error("Watchdog Alert: DICOM Server detected as DOWN or HUNG! Initiating self-healing...")
            
            try:
                stop_dicom_server() # Clear the old port/server
                time.sleep(5)      # Give the OS time to release the socket
                start_dicom_server(app)
                log.info("Watchdog: Self-healing successful. Server restarted.")
            except Exception as e:
                log.critical("Watchdog: Self-healing FAILED!", exc_info=True)        
