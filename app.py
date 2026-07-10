import os
import sys
# ====================================================================
if __name__ == '__main__':
    sys.modules['app'] = sys.modules['__main__']
# ====================================================================
import time
import shutil
import signal
import threading
import subprocess
import psutil
from io import BytesIO
from flask import Flask, render_template, redirect, request, session, url_for, flash, jsonify
import pydicom
from pydicom.errors import InvalidDicomError
from sqlalchemy.exc import IntegrityError

# Single, clean import from database.py
from core.database import (db, PatientStudy, queue_study_update, file_lock, db_lock, 
                      db_queue, AllowedScanner, AllowedModality, ImageInstance, 
                      AuditLog, ModalityZipRule, DescriptionRule, DescriptionMapping, 
                      Center, InstitutionMapping, CenterModality, SystemSetting, ReferringPhysician, ModalityCompressionRule, PipelineStatus)

from ingestion.dicom_receiver import start_dicom_server, stop_dicom_server, get_custom_path, invalidate_modality_cache, dicom_watchdog_loop
from services.metadata_engine import engine as metadata_engine
from config import Config
from logger import get_enterprise_logger
from activity_tracker import tracker
import webview


app = Flask(__name__)


log = get_enterprise_logger("WebApp")



app.config.from_object(Config)        
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SECRET_KEY'] = getattr(Config, 'SECRET_KEY', 'fallback_super_secret_key_change_in_production')
# --- NEW: MULTI-THREADING DB CONFIG ---
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
    'connect_args': {'check_same_thread': False}
}
# --------------------------------------
app.config['MAX_CONTENT_LENGTH'] = 5000 * 1024 * 1024 

app.config['MAX_FORM_MEMORY_SIZE'] = 5000 * 1024 * 1024 

app.config['MAX_FORM_PARTS'] = 100000
db.init_app(app)
INCOMING_FOLDER = Config.INCOMING_FOLDER
# --- SSE REAL-TIME TRIGGERS ---
from sqlalchemy import event
from sse_notifier import add_client, remove_client, notify_study_change

@event.listens_for(PatientStudy, 'after_update')
def study_after_update(mapper, connection, target):
    notify_study_change(target.study_uid)

@event.listens_for(PatientStudy, 'after_insert')
def study_after_insert(mapper, connection, target):
    notify_study_change(target.study_uid)
# ------------------------------


#routes
import requests # Make sure this is imported at the top of app.py!

def authenticate_gateway_on_boot(app):
    """
    Runs during application startup (or when credentials are changed). 
    Validates them against the central server and saves the state to app.config.
    """
    with app.app_context():
        from core.database import get_system_setting
        db_user = SystemSetting.query.filter_by(key='gateway_username').first()
        db_pass = SystemSetting.query.filter_by(key='gateway_password').first()
        auth_url = get_system_setting('AUTH_API_URL', 'https://fallback-url.com')

        if not db_user or not db_pass or not db_user.value or not db_pass.value:
            log.warning("⚠️ Gateway credentials missing or incomplete. Booting into Setup Mode.")
            app.config['AUTH_STATUS'] = "NEEDS_SETUP"  # <-- Save state
            return "NEEDS_SETUP"

        username = db_user.value
        password = db_pass.value
        headers = {"username": username, "password": password}

        log.info(f"Initiating Machine-to-Machine Auth for gateway: {username}...")

        try:
            response = requests.post(auth_url, headers=headers, timeout=15,  verify=False )
            
            try:
                response_data = response.json()
            except ValueError:
                response_data = {}

            if response.status_code == 200 and response_data.get("success") is True:
                log.info("✅ Gateway successfully authenticated with Central Server.")
                app.config['AUTH_STATUS'] = True  # <-- Save state
                return True
            else:
                api_error = response_data.get("errorMessage", response_data.get("message", "Unknown API Rejection"))
                log.critical(f"❌ BOOT HALTED: Central Server rejected credentials. Reason: {api_error}")
                app.config['AUTH_STATUS'] = False # <-- Save state
                return False

        except requests.exceptions.RequestException as e:
            log.critical(f"❌ BOOT HALTED: Cannot reach Central Server. Error: {e}")
            app.config['AUTH_STATUS'] = False     # <-- Save state
            return False

# ==========================================
# 1. THE SETUP & NETWORK LOCK (Global Bouncer)
# ==========================================
@app.before_request
def enforce_setup():
    """
    Ensures NO ONE can access the main dashboard until linked AND online.
    Allows Admin access to configure the system.
    """
    # 1. Whitelist ALL admin and setup endpoints
    allowed_routes = [
        'initial_setup', 
        'static', 
        'admin_login', 
        'unified_admin', 
        'settings',
        'zip_rules',       
        'add_mapping',     
        'manage_centers',
        'delete_mapping', 
        'admin_logout',    
        'logout',
        'authenticate_gateway',                     # <-- ADDED SO YOU CAN LOG OUT OF MAIN DASHBOARD
        'shutdown'                                  # <-- WHITELIST SHUTDOWN TO PREVENT FREEZING
    ]
    
    # If the user is trying to access a whitelisted admin/setup page, let them through
    if request.endpoint in allowed_routes:
        return

    # 2. Check the Global Auth Status we saved during boot
    auth_status = app.config.get('AUTH_STATUS')

    # 3. ROUTE 2 LOGIC: No credentials exist yet
    if auth_status == "NEEDS_SETUP":
        return redirect(url_for('initial_setup'))
        
    # 4. ROUTE 3 LOGIC: Credentials exist, but they failed (or network is dead)
    if auth_status is False:
        flash("⚠️ Gateway is offline or credentials failed. Dashboard locked. Please fix the API Connection.", "error")
        return redirect('/admin')
    
  #----------------------------------------------------------------===========
    

# ==========================================
# 2. ADMIN LOGIN & LOGOUT
# ==========================================
@app.route('/admin-login', methods=['GET', 'POST'])
def admin_login():
    """Password protection using the local Config password."""
    
    # 1. THE UX FIX: If they are already logged in, bounce them straight to the dashboard!
    if session.get('admin_auth'):
        return redirect('/admin')

    if request.method == 'POST':
        password_attempt = request.form.get('password')
        
        # 2. THE FLASK FIX: Use app.config to guarantee environment variables are respected
        expected_password = app.config.get('ADMIN_PASSWORD', 'admin123')
        
        # Ensure the attempt isn't blank before checking
        if password_attempt and password_attempt == expected_password:
            session['admin_auth'] = True 
            return redirect('/admin')
        else:
            flash("Invalid Local Admin Password.", "error")
            
    return render_template('admin_login.html')


@app.route('/admin-logout')
def admin_logout():
    session.pop('admin_auth', None)
    return redirect('/')


# ==========================================
# 3. THE UNIFIED MASTER ROUTE
# ==========================================
@app.route('/admin', methods=['GET'])
def unified_admin():
    """Fetches ALL data for the master tabbed interface."""
    # 1. Bounce them if they don't have the admin wristband
    if not session.get('admin_auth'):
        return redirect('/admin-login')

    # 2. Check Cloud Link Status (Requires BOTH username and password)
    gateway_user = SystemSetting.query.filter_by(key='gateway_username').first()
    gateway_pass = SystemSetting.query.filter_by(key='gateway_password').first()
    
    # Strictly evaluate that both rows exist AND actually contain text
    has_creds = bool(
        gateway_user and gateway_user.value and 
        gateway_pass and gateway_pass.value
    )
    # NEW: Fetch all settings as a dictionary for the UI
    all_settings_raw = SystemSetting.query.all()
    sys_settings = {s.key: s.value for s in all_settings_raw}

    # 3. Gather all configuration data
    scanners = AllowedScanner.query.all()
    modalities = AllowedModality.query.all()
    rules = DescriptionRule.query.all()
    mappings = DescriptionMapping.query.all()
    centers = Center.query.all()
    zip_rules = ModalityZipRule.query.all()
    comp_rules = ModalityCompressionRule.query.all()
    
    from systemProfile import SystemProfile
    import os
    is_legacy = SystemProfile.is_legacy_node()
    sys_limits = {
        'max_conn': 2 if is_legacy else 50,
        'max_comp_threads': 1 if is_legacy else max(2, (os.cpu_count() or 2) - 1),
        'max_upload_threads': 1 if is_legacy else 16
    }
    
    return render_template('unified_admin.html', 
                           scanners=scanners, 
                           modalities=modalities, 
                           rules=rules, 
                           mappings=mappings, 
                           centers=centers, 
                           zip_rules=zip_rules,
                           comp_rules=comp_rules,
                           has_creds=has_creds, 
                           gateway_user=gateway_user,
                           sys_settings=sys_settings,
                           sys_limits=sys_limits)


import urllib3

# Suppress the insecure request warnings since we are bypassing SSL verification
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from mapper import get_mapped_rule # <-- Add this import if it isn't at the top!

@app.route('/api/patients/<study_uid>/push_updates', methods=['POST'])
def push_study_updates(study_uid):
    # 1. Fetch the patient from your local database
    study = db.session.get(PatientStudy, study_uid)
    if not study:
        return jsonify({"error": "Study not found"}), 404

    # Get the specific field the user just updated
    data = request.json or {}
    update_type = data.get('update_type')

    # The payload MUST always contain the UID so the remote server knows who to update
    entity = {
        "studyInstanceUID": study.study_uid
    }

    # 2. DYNAMIC PAYLOAD BUILDER: Only append the field that was actually changed
    if update_type == 'physician':
        entity["refPhysicianName"] = study.ref_physician_name or ""
        log.info(f"Syncing ONLY Physician Name for {study_uid}")

    elif update_type == 'history':
        entity["history"] = study.patient_history or ""
        log.info(f"Syncing ONLY Patient History for {study_uid}")

    elif update_type == 'description':
        # --- Resolve Rule ID for Description ---
        rule_id_string = ""
        official_desc_string = ""
        
        # 1. Try to get it from the strict mapping table first
        mapped_ids, mapped_texts = get_mapped_rule(study.study_description, study.modality)
        
        if mapped_ids:
            rule_id_string = mapped_ids
            official_desc_string = mapped_texts
        elif study.outgoing_description:
            # 2. Fallback: Parse the saved text
            official_desc_string = study.outgoing_description
            
            # ATTEMPT A: Try matching the exact string WITHOUT stripping it first
            exact_rule = DescriptionRule.query.filter_by(original_description=study.outgoing_description).first()
            
            if exact_rule:
                rule_id_string = str(exact_rule.rule_id)
            else:
                # ATTEMPT B: If it fails, split it and use fuzzy matching
                desc_list = [d.strip() for d in study.outgoing_description.split(',')]
                matched_ids = []
                for desc in desc_list:
                    if desc: # Skip empty chunks
                        # Use wildcards (%) to ignore hidden newlines in the database
                        rule = DescriptionRule.query.filter(DescriptionRule.original_description.ilike(f"%{desc}%")).first()
                        if rule:
                            matched_ids.append(str(rule.rule_id))
                
                if matched_ids:
                    rule_id_string = ",".join(matched_ids)
                    
        # 🚨 CRITICAL FIX: Sanitize the final string for the Central API
        # This removes the \n before it gets converted into JSON
        if official_desc_string:
            official_desc_string = official_desc_string.replace('\n', ' ').replace('\r', '').strip()
            
        entity["studyDescriptionID"] = rule_id_string
        entity["studyDescription"] = official_desc_string 
        
        log.info(f"Syncing Description ID: [{rule_id_string}] and Text: [{official_desc_string}] for {study_uid}")

    # 3. Construct final payload
    payload = {
        "studyRequestEntity": entity
    }
    
    import json
    log.info(f"Pushing partial update to Central API for {study_uid}: {json.dumps(payload)}")

    from core.database import get_system_setting
    update_api_url = get_system_setting(
        'UPDATE_STUDY_API_URL', 
        'https://icare.anikrafoundation.com:8443/weasisintegratorwf/studymasterservice/updateStudy/'
    )
    headers = {"Content-Type": "application/json"}
    
    try:
        response = requests.post(update_api_url, json=payload, headers=headers, timeout=15, verify=False)
        if response.status_code == 200:
            response_data = response.json()
            if response_data.get("success") is True:
                return jsonify({"success": True, "message": "Successfully synced with Central Server!"})
            else:
                log.error(f"Central API returned 200 but failed logically: {response.text}")
                return jsonify({"error": "API received data but returned a failure status."}), 500
        else:
            log.error(f"Central API rejected update: {response.status_code} - {response.text}")
            return jsonify({"error": f"API rejected the update (HTTP {response.status_code})"}), 500
            
    except requests.exceptions.RequestException as e:
        log.error(f"Failed to reach Central API: {e}", exc_info=True)
        return jsonify({"error": "Could not connect to Central API"}), 500

#================================================================================================================
@app.route('/')
def home():
    
    from datetime import datetime, timedelta
    from core.database import get_system_setting
    search_text = request.args.get('search_query', '')
    modality_filter = request.args.get('modality_filter', '')
    date_filter = request.args.get('date_filter')
    if date_filter is None:
        date_filter = datetime.now().strftime('%Y-%m-%d')

    query = PatientStudy.query

    if search_text:
        search_words = search_text.split()
        
        for word in search_words:
            search_term = f"%{word}%"
            query = query.filter(
                (PatientStudy.patient_name.ilike(search_term)) | 
                (PatientStudy.patient_id.ilike(search_term)) |
                (PatientStudy.study_description.ilike(search_term))
        )
  
    if modality_filter:
        query = query.filter(PatientStudy.modality == modality_filter)

    if date_filter and date_filter != 'all':
        try:
            # Create a 24-hour window for the selected date
            start_of_day = datetime.strptime(date_filter, '%Y-%m-%d')
            end_of_day = start_of_day + timedelta(days=1)
            
            query = query.filter(
                PatientStudy.last_received >= start_of_day, 
                PatientStudy.last_received < end_of_day
            )
        except ValueError:
            pass # Failsafe: Ignore if someone types garbage into the URL

    studies = query.order_by(PatientStudy.last_received.desc()).all()
    center_name = get_system_setting('LOCAL_CENTER_NAME', 'DICOM Study Dashboard')
    center_code = get_system_setting('LOCAL_CENTER_CODE', '')
    # Pass the date_filter back to the template so the calendar picker remembers the selection
    return render_template('index.html', 
                           studies=studies, 
                           search_text=search_text, 
                           modality=modality_filter, 
                           date_filter=date_filter,
                           center_name=center_name,       
                           center_code=center_code)

@app.route('/api/system/reset', methods=['POST'])
def reset_system():
    """Nuclear option to wipe all studies, files, and queues."""
    # 1. Clear Memory Queues to prevent zombie files
    from core.event_bus import db_write_queue, compression_queue, zip_queue, upload_queue
    try:
        with db_write_queue.mutex: db_write_queue.queue.clear()
        with compression_queue.mutex: compression_queue.queue.clear()
        with zip_queue.mutex: zip_queue.queue.clear()
        with upload_queue.mutex: upload_queue.queue.clear()
    except Exception:
        pass # Ignore queue errors if any

    # 2. Clear Database
    from core.database import db, PatientStudy, Series, ImageInstance, AuditLog
    try:
        db.session.query(ImageInstance).delete()
        db.session.query(Series).delete()
        db.session.query(PatientStudy).delete()
        db.session.query(AuditLog).delete()
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        return jsonify({"success": False, "error": f"DB Error: {e}"}), 500

    # 3. Wipe Disk (Incoming, Processing, Outgoing, Backup, Archive, and Error)
    import shutil
    folders_to_wipe = [
        Config.INCOMING_FOLDER,
        Config.PROCESSING_FOLDER,
        Config.OUTGOING_FOLDER,
        Config.BACKUP_FOLDER,
        Config.ARCHIVE_FOLDER,
        getattr(Config, 'ERROR_FOLDER', './error_data')
    ]
    for folder in folders_to_wipe:
        if os.path.exists(folder):
            for filename in os.listdir(folder):
                file_path = os.path.join(folder, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                except Exception:
                    pass # Skip locked files

    return jsonify({"success": True, "message": "System fully reset."})

@app.route('/delete/<uid>')
def delete_study(uid):
    # Fetch the study from the database before deleting it so we can read its metadata
    study_to_delete = PatientStudy.query.filter_by(study_uid=uid).first()
    
    if study_to_delete:
        # =================================================================
        # 1. SETUP HACKER-PROOF DATA PATHS
        # =================================================================
        def secure_string(text, default="Unknown"):
            safe_chars = [c for c in str(text) if c.isalnum() or c in (' ', '_', '-', '.')]
            safe_text = "".join(safe_chars).strip().lstrip('.')
            return safe_text if safe_text else default

        clean_name = secure_string(study_to_delete.patient_name, "Unknown_Patient")
        clean_uid = secure_string(uid, "Unknown_UID")
        
        # This perfectly matches the new folder logic in receiver.py
        flat_folder_name = f"{clean_name}_{clean_uid}"
        
        # --- PATH DEFINITIONS ---
        incoming_target = os.path.join(getattr(Config, 'INCOMING_FOLDER', 'Incoming'), flat_folder_name)
        backup_target = os.path.join(getattr(Config, 'BACKUP_FOLDER', 'Backup_Data'), flat_folder_name)
        
        outbound_base = getattr(Config, 'OUTGOING_FOLDER', 'data/outgoing')
        staging_target = os.path.join(outbound_base, 'staging', uid)
        
        # --- FIND ORPHANED ZIPS ---
        zips_to_delete = []
        if os.path.exists(outbound_base):
            for f in os.listdir(outbound_base):
                # 🚨 THE FIX: Simply check if the Study UID is inside the ZIP filename!
                if f.endswith('.zip') and uid in f:
                    zips_to_delete.append(os.path.join(outbound_base, f))

        # =================================================================
        # 2. THE HARD DRIVE SWEEP
        # =================================================================
        try:
            import shutil
            with file_lock:
                # Sweep A: Incoming Folder
                if os.path.exists(incoming_target):
                    shutil.rmtree(incoming_target)
                    
                # Sweep B: Backup Folder (Crucial for saving disk space!)
                if os.path.exists(backup_target):
                    shutil.rmtree(backup_target)
                    
                # Sweep C: Staging Folder (If pipeline crashed during compression)
                if os.path.exists(staging_target):
                    shutil.rmtree(staging_target)
                    log.info("Swept leftover staging data during delete", extra={"study_uid": uid})
                    
                # Sweep D: Orphaned ZIP chunks (If pipeline crashed during upload)
                for zip_path in zips_to_delete:
                    if os.path.exists(zip_path):
                        try:
                            os.remove(zip_path)
                            log.info(f"Swept leftover zip chunk: {os.path.basename(zip_path)}")
                        except PermissionError:
                            with open(zip_path + ".deleted", "w") as f:
                                f.write("deleted")
                            log.warning(f"File {zip_path} locked by background worker. Dropped anti-zombie marker.")
                        
        except Exception as e:
            log.error("Failed to clear some physical folders during study deletion", extra={"study_uid": uid}, exc_info=True)
            flash(f"Warning: Database record deleted, but failed to remove all files from the hard drive.", "warning")

        # =================================================================
        # 3. DATABASE DELETION
        # =================================================================
        with db_lock:
            db.session.delete(study_to_delete)
            db.session.add(AuditLog(
                action_type='MANUAL_DELETE', 
                study_uid=uid, 
                source='WEB_DASHBOARD'
            ))
            db.session.commit()
            
        log.info("Database record and all associated physical files safely wiped.", extra={"study_uid": uid})
        flash("Study and all leftover files successfully deleted.", "success")
    else:
        flash("Study not found in the database.", "danger")

    return redirect('/')


from core.database import PipelineStatus # Make sure you have this if using Enum statuses!

@app.route('/api/patients/<study_uid>/toggle_pause', methods=['POST'])
def toggle_pause(study_uid):
    """Toggles the pause state of a study without requiring an Admin login."""
    try:
        with db_lock:
            study = PatientStudy.query.filter_by(study_uid=study_uid).first()
            if not study:
                return jsonify({"success": False, "error": "Study not found"}), 404

            # 1. Flip the pause switch
            # (Note: This requires you to have 'is_paused = db.Column(db.Boolean, default=False)' in database.py)
            study.is_paused = not study.is_paused
            
            # 2. Pipeline Queue Management
            if not study.is_paused:
                # If we just RESUMED it, push it back into the active pipeline
                if study.upload_status in [PipelineStatus.FAILED, PipelineStatus.PAUSED]:
                    study.upload_status = PipelineStatus.PENDING
                    study.retry_count = 0
            else:
                # If we just PAUSED it, and it hasn't started uploading yet, pull it from the queue
                if study.upload_status == PipelineStatus.PENDING:
                    study.upload_status = PipelineStatus.PAUSED

            db.session.commit()
            
        return jsonify({"success": True, "is_paused": study.is_paused})
        
    except Exception as e:
        log.error(f"Failed to toggle pause for {study_uid}: {str(e)}", exc_info=True)
        return jsonify({"success": False, "error": "Internal database error"}), 500

@app.route('/api/activity', methods=['GET'])
def get_live_activity():
    """
    Frontend UI polls this endpoint every 2 seconds to update the floating box.
    """
    # Grab the string from RAM (instantaneous O(1) time complexity)
    current_message = tracker.get_activity()
    
    return {"activity": current_message}, 200





from core.database import AllowedModality 

from core.database import AllowedModality, ImageInstance, db_lock # <-- Ensure ImageInstance and db_lock are imported!

@app.route('/upload', methods=['POST'])
def upload_file():
    # 1. SECURITY GATE
    uploaded_files = request.files.getlist('dicom_files')
    if not uploaded_files or uploaded_files[0].filename == '':
        return jsonify({"success": False, "error": "No files selected"}), 400

    processed_count = 0
    failed_count = 0
    error_messages = []

    # Helper functions...
    def safe_cast(val, cast_type, default=None):
        if val is None or str(val).strip() == '': return default
        try: return cast_type(val)
        except: return default

    def extract_list_string(ds, tag_name):
        val = getattr(ds, tag_name, "")
        if isinstance(val, list) or type(val).__name__ == 'MultiValue':
            return "\\".join([str(v) for v in val])
        return str(val)

    for file in uploaded_files:
        if file.filename == '': continue

        try:
            # 2. FAST READ (Stay in RAM)
            file_bytes = file.read()
            ds = pydicom.dcmread(BytesIO(file_bytes), force=True)
            start_time = time.time()
            
            # 3. CRITICAL UIDS & VALIDATION
            s_uid = str(getattr(ds, 'StudyInstanceUID', 'UNKNOWN'))
            series_uid = str(getattr(ds, 'SeriesInstanceUID', 'UNKNOWN'))
            sop_uid = str(getattr(ds, 'SOPInstanceUID', 'UNKNOWN'))
            
            if "UNKNOWN" in (s_uid, series_uid, sop_uid):
                failed_count += 1
                error_messages.append(f"{file.filename}: Missing critical UIDs")
                continue


            # 4. MODALITY FIREWALL
            mod = str(getattr(ds, 'Modality', 'Unknown')).strip().upper()
            allowed = AllowedModality.query.filter_by(name=mod).first()
            if not allowed:
                failed_count += 1
                error_messages.append(f"{file.filename}: Unauthorized Modality ({mod})")
                from config import Config
                import os
                error_dir = getattr(Config, 'ERROR_FOLDER', './error_data')
                os.makedirs(error_dir, exist_ok=True)
                ds.save_as(os.path.join(error_dir, sop_uid + '.dcm'), write_like_original=False)
                continue

            # 5. METADATA EXTRACTION
            p_name_raw = getattr(ds, 'PatientName', 'Unknown')
            p_name = str(p_name_raw).replace('^', ' ') if type(p_name_raw).__name__ == 'PersonName' else str(p_name_raw)

            raw_date = str(getattr(ds, 'StudyDate', ''))
            formatted_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}" if len(raw_date) == 8 else "Unknown"

            # 6. PATH CALCULATION (Using Enterprise Naming Convention!)
            enterprise_filename = f"{mod}.{sop_uid}.dcm"
            incoming_dir = get_custom_path(Config.INCOMING_FOLDER, p_name, s_uid)
            backup_dir = get_custom_path(Config.BACKUP_FOLDER, p_name, s_uid)

            # 7. ATOMIC DUAL-WRITE STRATEGY (BARE-METAL MODE)
            with file_lock:
                final_incoming = os.path.join(incoming_dir, enterprise_filename)
                temp_incoming = final_incoming + ".tmp"
                
                final_backup = os.path.join(backup_dir, enterprise_filename)
                temp_backup = final_backup + ".tmp"

                # CRITICAL FIX 2: Write the exact raw bytes to disk. Never use ds.save_as() here!
                if not os.path.exists(final_incoming):
                    with open(temp_incoming, 'wb') as f:
                        f.write(file_bytes)
                    os.rename(temp_incoming, final_incoming)

                if not os.path.exists(final_backup):
                    with open(temp_backup, 'wb') as f:
                        f.write(file_bytes)
                    os.rename(temp_backup, final_backup)

            # 8. DATABASE SYNC PAYLOAD
            payload = {
                'start_time': start_time,
                'temp_incoming': temp_incoming,
                'final_incoming': final_incoming,
                
                'study_uid': s_uid,
                'patient_id': str(getattr(ds, "PatientID", "Unknown")),
                'patient_name': p_name,
                'patient_age': str(getattr(ds, "PatientAge", "N/A")),
                'patient_sex': str(getattr(ds, "PatientSex", "O")),
                'patient_birth_date': str(getattr(ds, "PatientBirthDate", "")),
                'modality': mod,
                'study_date': formatted_date,
                'study_description': metadata_engine.extract_best_study_description(ds),
                'institution_name': str(getattr(ds, "InstitutionName", "")),
                'ref_physician_name': str(getattr(ds, "ReferringPhysicianName", "")).replace('^', ' '),
                'accession_number': str(getattr(ds, "AccessionNumber", "")),
                'manufacturer': str(getattr(ds, "Manufacturer", "")),
                'manufacturer_model_name': str(getattr(ds, "ManufacturerModelName", "")),
                
                'series_uid': series_uid,
                'series_number': safe_cast(getattr(ds, "SeriesNumber", None), int, 0),
                'series_description': str(getattr(ds, "SeriesDescription", "")),
                
                'sop_uid': sop_uid,
                'instance_number': safe_cast(getattr(ds, "InstanceNumber", None), int, 0),
                
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

            success = queue_study_update(payload)
            if success: 
                processed_count += 1
            else:
                failed_count += 1
                error_messages.append(f"{file.filename}: Database queue full")

        except Exception as e:
            failed_count += 1
            error_messages.append(f"{file.filename}: Internal processing error")
            log.error(f"Manual upload failed: {e}", exc_info=True)

    # 9. RESPOND WITH CLEAN JSON
    return jsonify({
        "success": processed_count > 0,
        "processed": processed_count,
        "failed": failed_count,
        "errors": error_messages
    })




@app.route('/upload-page')
def upload_page():
    return render_template('upload.html')





@app.route('/zip-rules', methods=['POST'])
def zip_rules():
    # 1. Security Gate: Only authenticated admins can post data here
    if not session.get('admin_auth'): 
        return redirect('/admin-login')
        
    action = request.form.get('action')

    # --- THE UPSERT LOGIC (Add or Edit) ---
    if action == 'save':
        modality_name = request.form.get('modality_name').strip().upper()
        try:
            # Force the input to be an integer. If they type "abc", this throws an error.
            limit = int(request.form.get('limit'))
            
            if limit <= 0:
                flash("Limit must be greater than zero.", "danger")
            elif modality_name:
                # Ask the DB: Does this rule already exist?
                existing_rule = ModalityZipRule.query.filter_by(modality_name=modality_name).first()
                
                if existing_rule:
                    # UPDATE
                    existing_rule.max_files_per_zip = limit
                    log.info("Zip rule updated", extra={"modality": modality_name, "new_limit": limit})
                    flash(f"Updated rule: {modality_name} now zips at {limit} files.", "success")
                else:
                    # INSERT
                    new_rule = ModalityZipRule(modality_name=modality_name, max_files_per_zip=limit)
                    db.session.add(new_rule)
                    log.info("New zip rule created", extra={"modality": modality_name, "limit": limit})
                    flash(f"Created rule: {modality_name} set to {limit} files.", "success")
                    
                db.session.commit()
        except ValueError:
            flash("Invalid limit. Please enter a valid number.", "danger")
        except Exception as e:
            db.session.rollback()
            log.error("Failed to save zip rule", exc_info=True)
            flash("Database error occurred while saving the rule.", "danger")

    # --- THE DELETE LOGIC ---
    elif action == 'delete':
        rule_id = request.form.get('rule_id')
        rule = db.session.get(ModalityZipRule, rule_id)
        if rule:
            db.session.delete(rule)
            db.session.commit()
            log.info("Zip rule deleted", extra={"rule_id": rule_id})
            flash("Zip rule successfully removed.", "success")

    # Always redirect back to the unified admin dashboard when finished!
    return redirect('/admin')


@app.route('/settings', methods=['POST'])
def settings():
    # 1. Security Gate: Only authenticated admins can post data here
    if not session.get('admin_auth'): 
        return redirect('/admin-login')
        
    action = request.form.get('action')
    
    # ==========================================
    # CLOUD CREDENTIALS MANAGEMENT
    # ==========================================
    if action == 'save_credentials':
        username = request.form.get('api_username').strip()
        password = request.form.get('api_password').strip()

        if username and password:
            with db_lock:
                # Upsert Username
                u_setting = SystemSetting.query.filter_by(key='gateway_username').first()
                if not u_setting:
                    u_setting = SystemSetting(key='gateway_username')
                    db.session.add(u_setting)
                u_setting.value = username

                # Upsert Password
                p_setting = SystemSetting.query.filter_by(key='gateway_password').first()
                if not p_setting:
                    p_setting = SystemSetting(key='gateway_password')
                    db.session.add(p_setting)
                p_setting.value = password

                db.session.commit()
            log.info("Central API credentials updated via Admin panel.")
            flash("✅ Gateway Credentials saved successfully. Please restart the app to resume syncing.", "success")
        else:
            flash("Both username and password are required.", "error")

    elif action == 'revoke_credentials':
        with db_lock:
            # Safely delete both credential rows from the database
            SystemSetting.query.filter(SystemSetting.key.in_(['gateway_username', 'gateway_password'])).delete(synchronize_session=False)
            db.session.commit()
        log.warning("Central API credentials revoked via Admin panel.")
        flash("❌ Credentials revoked. The gateway will no longer sync with the central server.", "error")

    # ==========================================
    # HARDWARE FIREWALL: VIP SCANNERS
    # ==========================================
    elif action == 'add':
        ae_title = request.form.get('ae_title').strip().upper()
        ip_address = request.form.get('ip_address').strip()
        port = int(request.form.get('port').strip()) 
        description = request.form.get('description').strip()
        
        if ae_title and ip_address and port:
            existing = AllowedScanner.query.filter_by(ae_title=ae_title, ip_address=ip_address, port=port).first()
            if not existing:
                new_scanner = AllowedScanner(ae_title=ae_title, ip_address=ip_address, port=port, description=description)
                db.session.add(new_scanner)
                db.session.commit()
                log.info("New authorized scanner added", extra={"ae_title": ae_title, "ip": ip_address, "port": port})
                flash(f"Scanner '{ae_title}' successfully added to the VIP list.", "success")

    elif action == 'delete':
        scanner_id = request.form.get('scanner_id')
        scanner = db.session.get(AllowedScanner, scanner_id)
        if scanner:
            db.session.delete(scanner)
            db.session.commit()
            log.info("Authorized scanner removed", extra={"scanner_id": scanner_id})
            flash("Scanner successfully removed from the VIP list.", "success")

    
    elif action == 'toggle_security':
        is_strict = request.form.get('STRICT_AE_IP_CHECK')
        with db_lock:
            setting = SystemSetting.query.filter_by(key='STRICT_AE_IP_CHECK').first()
            if not setting:
                setting = SystemSetting(key='STRICT_AE_IP_CHECK')
                db.session.add(setting)
            setting.value = is_strict
            db.session.commit()
        
        if is_strict == 'true':
            log.info("DICOM Security Firewall ENABLED via Admin panel.")
            flash("🛡️ DICOM Security Firewall is now ENABLED. Only VIP Scanners can send data.", "success")
        else:
            log.warning("DICOM Security Firewall DISABLED via Admin panel.")
            flash("⚠️ DICOM Security Firewall is now DISABLED. All incoming network connections are allowed.", "error")
    # ==========================================
    # MODALITY FIREWALL
    # ==========================================
    elif action == 'add_modality':
        modality_code = request.form.get('modality_code').strip().upper() 
        
        if modality_code:
            existing = AllowedModality.query.filter_by(name=modality_code).first()
            if not existing:
                new_mod = AllowedModality(name=modality_code)
                db.session.add(new_mod)
                db.session.commit()
                invalidate_modality_cache() # Clear RAM cache
                log.info("New authorized modality added", extra={"modality": modality_code})
                flash(f"Modality '{modality_code}' successfully whitelisted.", "success")

    elif action == 'delete_modality':
        mod_id = request.form.get('modality_id')
        mod = db.session.get(AllowedModality, mod_id)
        if mod:
            db.session.delete(mod)
            db.session.commit()
            invalidate_modality_cache() # Clear RAM cache
            log.info("Authorized modality removed", extra={"modality_id": mod_id})
            flash("Modality successfully removed from the whitelist.", "success")


    elif action == 'update_urls':
        keys_to_update = [
            'AUTH_API_URL', 'CREATE_STUDY_API_URL', 'UPDATE_STUDY_API_URL', 
            'DESCRIPTION_API_URL', 'TUS_API_URL', 'TUS_API_USER', 'TUS_API_KEY', 'UL_CHUNK', 
            'UL_WORKERS', 'UL_MAX_RETRIES', 'DICOM_PORT', 'MAX_CONCURRENT_CONNECTIONS', 
            'DICOM_AE_TITLE', 'HOST', 'STUDY_COOLDOWN_SECONDS',
            'DB_RETENTION_DAYS', 'BACKUP_RETENTION_DAYS', 'WORKING_RETENTION_DAYS',
            'LOCAL_CENTER_NAME', 'LOCAL_CENTER_CODE', 'COMPRESSION_WORKERS', 'ZIP_MODE'
        ]
        
        log.info(f"INCOMING PAYLOAD: {request.form}")
        
        try:
            # 1. Aggressively sever any tangled read transactions on the web thread
            db.session.rollback() 
            
            log.info("Attempting to acquire db_lock...")
            with db_lock:
                log.info("Lock acquired! Updating settings...")
                for key in keys_to_update:
                    val = request.form.get(key)
                    if val is not None:
                        setting = SystemSetting.query.filter_by(key=key).first()
                        if not setting:
                            setting = SystemSetting(key=key)
                            db.session.add(setting)
                        setting.value = val.strip()
                
                log.info("Committing changes...")
                db.session.commit()
                
            log.info("✅ System URLs updated via Admin panel.")
            flash("✅ System URLs updated successfully!", "success")
            
        except Exception as e:
            db.session.rollback()
            log.error(f"CRASH during URL update: {str(e)}", exc_info=True)
            flash("❌ Failed to update settings. Check system logs.", "error")    
    # Always redirect back to the unified admin dashboard when finished!
    return redirect('/admin')

@app.route('/authenticate-gateway', methods=['POST'])
def authenticate_gateway():
    """Triggered manually from the dashboard to retry authentication."""
    # Security Gate
    if not session.get('admin_auth'): 
        return redirect('/admin-login')
        
    log.info("Manual authentication triggered via Dashboard...")
    
    # Re-run the auth check
    test_status = authenticate_gateway_on_boot(app)
    
    if test_status is True:
        # Success! Turn on the background processors dynamically
        start_enterprise_pipeline(app)
        flash("✅ Gateway successfully linked! The DICOM Pipeline is now running.", "success")
        return redirect('/') # Kick them straight to the main dashboard
    else:
        flash("❌ Central Server rejected credentials. Please check spelling and network connection.", "error")
        return redirect('/admin')



#====================================================================================
    # GET Request: Fetch both tables to display on the page
    scanners = AllowedScanner.query.all()
    modalities = AllowedModality.query.all() # Fetch the modalities!
    return render_template('settings.html', scanners=scanners, modalities=modalities)


from core.database import DescriptionRule, DescriptionMapping

# ==========================================
# DICTIONARY MAPPING ROUTE (POST ONLY)
# ==========================================
# ==========================================
# DICTIONARY MAPPING ROUTE (POST ONLY)
# ==========================================
@app.route('/add-mapping', methods=['POST'])
def add_mapping():
    # Security Gate
    if not session.get('admin_auth'): return redirect('/admin-login')
    
    m_desc = request.form.get('machine_desc', '').strip()
    
    # 🚨 THE FIX: Use getlist() to capture ALL checked boxes!
    rule_ids = request.form.getlist('rule_id')
    
    if m_desc and rule_ids:
        with db_lock:
            # 1. Clear any old mappings for this exact machine text to prevent duplicates
            DescriptionMapping.query.filter_by(machine_description=m_desc).delete()
            
            # 2. Create a new mapping row for EVERY checkbox the admin selected
            added_count = 0
            for r_id in rule_ids:
                if r_id.strip():
                    new_map = DescriptionMapping(machine_description=m_desc, rule_id=r_id.strip())
                    db.session.add(new_map)
                    added_count += 1
            
            db.session.commit()
            
        flash(f"Success: Mapped '{m_desc}' to {added_count} official rules.", "success")
    else:
        flash("Error: You must provide machine text and select at least one rule.", "error")
            
    return redirect('/admin')

# ==========================================
# DELETE DICTIONARY MAPPING ROUTE
# ==========================================
@app.route('/delete-mapping', methods=['POST'])
def delete_mapping():
    # Security Gate
    if not session.get('admin_auth'): 
        return redirect('/admin-login')
    
    mapping_id = request.form.get('mapping_id')
    
    if mapping_id:
        with db_lock:
            # Find the mapping by its database ID
            mapping = db.session.get(DescriptionMapping, mapping_id)
            if mapping:
                db.session.delete(mapping)
                db.session.commit()
                log.info("Mapping override deleted", extra={"mapping_id": mapping_id})
                flash("Mapping override successfully removed.", "success")
            else:
                flash("Mapping not found.", "error")
                
    return redirect('/admin')

# ==========================================
# CENTER ROUTING ROUTE (POST ONLY)
# ==========================================
# ==========================================
# CENTER ROUTING ROUTE (POST ONLY)
# ==========================================
@app.route('/admin/centers', methods=['POST'])
def manage_centers():
    # Security Gate
    if not session.get('admin_auth'): return redirect('/admin-login')
    
    action = request.form.get('action', 'add')

    try:
        with db_lock:
            # --- ACTION: ADD / UPSERT MAPPING ---
            if action == 'add':
                center_code = request.form.get('center_code').strip()
                dicom_name = request.form.get('dicom_institution_name').strip()
                selected_modalities = request.form.getlist('modalities') 

                if not center_code or not dicom_name:
                    flash("Center Code and DICOM Name are required.", "error")
                    return redirect('/admin')

                # 1. Find the Center, or Create it if it doesn't exist
                center = Center.query.filter_by(center_code=center_code).first()
                if not center:
                    # We save the code as the name as well to satisfy the database schema without needing a migration
                    center = Center(center_code=center_code, name=center_code)
                    db.session.add(center)
                    db.session.flush() # Force DB to generate the ID

                # 2. Upsert the DICOM Mapping (Attach it to the center)
                existing_mapping = InstitutionMapping.query.filter_by(dicom_institution_name=dicom_name).first()
                if not existing_mapping:
                    mapping = InstitutionMapping(dicom_institution_name=dicom_name, center_id=center.id)
                    db.session.add(mapping)
                elif existing_mapping.center_id != center.id:
                    # Move an existing mapping to this center
                    existing_mapping.center_id = center.id

                # 3. Add Modality Rules (Skip if already checked)
                for mod in selected_modalities:
                    existing_mod = CenterModality.query.filter_by(center_id=center.id, modality_name=mod).first()
                    if not existing_mod:
                        rule = CenterModality(center_id=center.id, modality_name=mod)
                        db.session.add(rule)

                # 4. Instantly unblock any pending studies on the UI
                blocked_studies = PatientStudy.query.filter_by(
                    institution_name=dicom_name, 
                    remote_upload_id='UNMAPPED_CENTER'
                ).all()
                for bs in blocked_studies:
                    bs.remote_upload_id = ''
                    bs.upload_status = PipelineStatus.PENDING

                db.session.commit()
                flash(f"Center {center_code} configured successfully!", "success")

            # --- ACTION: DELETE ENTIRE CENTER ---
            elif action == 'delete_center':
                center_id = request.form.get('center_id')
                center = db.session.get(Center, center_id)
                if center:
                    db.session.delete(center)
                    db.session.commit()
                    flash(f"Center {center.center_code} and all its mappings were deleted.", "success")

            # --- ACTION: DELETE SPECIFIC MAPPING ---
            elif action == 'delete_mapping':
                mapping_id = request.form.get('mapping_id')
                mapping = db.session.get(InstitutionMapping, mapping_id)
                if mapping:
                    db.session.delete(mapping)
                    db.session.commit()
                    flash("DICOM Mapping removed successfully.", "success")

    except IntegrityError:
        db.session.rollback()
        flash("Database Error: Failed to save routing configuration.", "error")
    except Exception as e:
        db.session.rollback()
        flash(f"System Error: {str(e)}", "error")
        
    return redirect('/admin')


# ==========================================
# INITIAL SETUP ROUTE (FIRST BOOT ONLY)
# ==========================================
@app.route('/setup', methods=['GET', 'POST'])
def initial_setup():
    """Standalone page for the very first time the Gateway boots."""
    # Ensure even the setup page is protected by the Local Admin password
    if not session.get('admin_auth'): return redirect('/admin-login')
    
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')

        if not username or not password:
            flash("Please provide both the API Username and Password.", "error")
            return redirect(url_for('initial_setup'))

        with db_lock:
            # Upsert Username
            u_setting = SystemSetting.query.filter_by(key='gateway_username').first()
            if not u_setting:
                u_setting = SystemSetting(key='gateway_username')
                db.session.add(u_setting)
            u_setting.value = username

            # Upsert Password
            p_setting = SystemSetting.query.filter_by(key='gateway_password').first()
            if not p_setting:
                p_setting = SystemSetting(key='gateway_password')
                db.session.add(p_setting)
            p_setting.value = password

            db.session.commit()

        # Test the connection immediately without restarting!
        test_status = authenticate_gateway_on_boot(app)
        if test_status is True:
            flash("✅ Node Linked Successfully! Please restart the Python application to begin processing DICOM files.", "success")
        else:
            flash("❌ Credentials saved, but the Central Server rejected them. Please check your spelling.", "error")

        return redirect(url_for('initial_setup'))

    # GET Request: Just render the setup page
    return render_template('admin_login.html')

@app.route('/api/patients/<study_uid>/update_physician', methods=['POST'])
def update_physician(study_uid):
    data = request.json
    new_name = data.get('physician_name', '').strip()

    # 1. Find the patient
    patient = db.session.get(PatientStudy, study_uid)
    if not patient:
        return jsonify({"error": "Patient not found"}), 404

    # 2. Update the patient's referring physician string directly
    patient.ref_physician_name = new_name

    # 3. If it's a new name, add it to the physician list so it appears in the dropdown next time
    if new_name:
        existing_doc = ReferringPhysician.query.filter(ReferringPhysician.name.ilike(new_name)).first()
        if not existing_doc:
            new_doc = ReferringPhysician(name=new_name)
            db.session.add(new_doc)

    db.session.commit()
    
    return jsonify({"success": True, "new_name": new_name})

# Route to fetch the list for the frontend dropdown
@app.route('/api/physicians/list', methods=['GET'])
def get_physician_list():
    physicians = ReferringPhysician.query.order_by(ReferringPhysician.name).all()
    return jsonify([p.name for p in physicians])



@app.route('/api/patients/<study_uid>/update_history', methods=['POST'])
def update_history(study_uid):
    data = request.json
    new_history = data.get('history', '').strip()

    # 1. Find the patient
    patient = db.session.get(PatientStudy, study_uid)
    if not patient:
        return jsonify({"error": "Patient not found"}), 404

    # 2. Update the patient's history
    patient.patient_history = new_history
    db.session.commit()
    
    return jsonify({"success": True, "new_history": new_history})


# Add this to app.py

@app.route('/api/descriptions/list', methods=['GET'])
def get_official_descriptions():
    """Fetches official API descriptions for the modal dropdown. Can filter by modality."""
    modality = request.args.get('modality')
    
    query = DescriptionRule.query
    if modality:
        query = query.filter_by(modality=modality)
        
    rules = query.order_by(DescriptionRule.original_description).all()
    
    return jsonify([
        {"rule_id": r.rule_id, "description": r.original_description} 
        for r in rules
    ])


@app.route('/api/patients/<study_uid>/update_outgoing_desc', methods=['POST'])
def update_outgoing_desc(study_uid):
    """Handles Case 2 (Update + Map) and Case 3 (Update without Mapping)."""
    data = request.json
    rule_ids_string = data.get('rule_id')       # Will now come in as "1,2"
    official_descs = data.get('description')    # Will now come in as "Desc A, Desc B"

    if not rule_ids_string or not official_descs:
        return jsonify({"error": "Missing rule ID or description"}), 400

    patient = db.session.get(PatientStudy, study_uid)
    if not patient:
        return jsonify({"error": "Patient not found"}), 404

    try:
        with db_lock:
            # 1. Update the patient's outgoing description
            patient.outgoing_description = official_descs

            # 2. Handle Mapping Logic (Cases 2 & 3)
            incoming_desc = patient.study_description

            if incoming_desc and incoming_desc.strip():
                # CASE 2: Incoming is NOT blank. Create mappings.
                # Clear old mappings for this string to prevent infinite duplicates
                DescriptionMapping.query.filter_by(machine_description=incoming_desc).delete()
                
                # Create a new mapping row for EVERY selected rule ID
                for r_id in rule_ids_string.split(','):
                    if r_id.strip():
                        new_mapping = DescriptionMapping(
                            machine_description=incoming_desc,
                            rule_id=r_id.strip()
                        )
                        db.session.add(new_mapping)
                log.info(f"New mappings created: '{incoming_desc}' -> Rules [{rule_ids_string}]")
            else:
                # CASE 3: Incoming is blank. Do not create a mapping.
                log.info(f"Study {study_uid} updated, but incoming description was blank. No mapping created.")

            db.session.commit()
            
        return jsonify({"success": True, "new_description": official_descs})
        
    except Exception as e:
        db.session.rollback()
        log.error(f"Error updating description: {str(e)}", exc_info=True)
        return jsonify({"error": "Internal database error"}), 500
    



from core.database import db_lock, PatientStudy, PipelineStatus

@app.route('/api/patients/<study_uid>/retry_upload', methods=['POST'])
def retry_upload(study_uid):
    """Resets the retry counter AND recovers stranded ZIP files from the hard drive."""
    try:
        with db_lock:
            study = PatientStudy.query.filter_by(study_uid=study_uid).first()
            if not study:
                return {"success": False, "error": "Study not found"}, 404

            # Reset the status back to PENDING and wipe the retry count
            study.upload_status = PipelineStatus.PENDING
            study.retry_count = 0
            
            if study.zip_status == PipelineStatus.FAILED:
                study.zip_status = PipelineStatus.PENDING
                
            db.session.commit()
            
        # --- THE RECOVERY FIX: Find orphaned ZIPs and push to queue ---
        import glob
        from core.event_bus import upload_queue
        
        outbound_base = getattr(Config, 'OUTGOING_FOLDER', 'data/outgoing')
        search_pattern = os.path.join(outbound_base, f"*_{study_uid}_contains_*.zip")
        failed_pattern = os.path.join(outbound_base, f"*_{study_uid}_contains_*.zip.failed")
        
        orphaned_zips = glob.glob(search_pattern)
        failed_zips = glob.glob(failed_pattern)
        
        # Recover .failed zips by renaming them back to .zip
        import shutil
        for failed_zip in failed_zips:
            try:
                recovered_zip = failed_zip[:-7] # Remove '.failed'
                shutil.move(failed_zip, recovered_zip)
                orphaned_zips.append(recovered_zip)
                log.info(f"Recovered failed archive: {recovered_zip}")
            except Exception as e:
                log.error(f"Could not recover failed archive {failed_zip}: {e}")
        
        queued_count = 0
        for zip_path in orphaned_zips:
            try:
                # Extract the chunk math: CT_123_contains_50_images_123.zip
                chunk_count = int(os.path.basename(zip_path).split('_contains_')[1].split('_images')[0])
            except Exception:
                chunk_count = 0
                
            upload_queue.put({
                'zip_path': zip_path,
                'study_uid': study_uid,
                'image_count': chunk_count
            })
            queued_count += 1
            
        log.info(f"Retry Upload: Recovered and queued {queued_count} stranded zip files for {study_uid}")

        return {"success": True, "message": f"Queued {queued_count} chunks!"}
    except Exception as e:
        return {"success": False, "error": str(e)}, 500
    

from flask import Response

@app.route('/api/stream')
def stream():
    """The Real-Time SSE Connection Pipeline"""
    def event_stream():
        import queue
        q = queue.Queue(maxsize=100)
        add_client(q)
        try:
            while True:
                # Blocks efficiently until a DB trigger wakes it up
                msg = q.get() 
                yield f"data: {msg}\n\n"
        except GeneratorExit:
            remove_client(q)
    return Response(event_stream(), content_type='text/event-stream')

@app.route('/api/render_row/<study_uid>')
def render_row(study_uid):
    """Renders just the raw HTML for a single table row."""
    study = db.session.get(PatientStudy, study_uid)
    if not study:
        return "", 404
    return render_template('study_row.html', s=study)



@app.route('/compression-rules', methods=['POST'])
def handle_compression_rules():
    # Security check (ensure admin is logged in, use your existing auth logic)
    
    action = request.form.get('action')
    
    with db_lock:
        try:
            if action == 'save':
                modality_name = request.form.get('modality_name')
                comp_type = request.form.get('compression_type', 'LOSSLESS')
                comp_level = int(request.form.get('compression_level', 100))
                
                if not modality_name:
                    flash("Modality is required.", "error")
                    return redirect(url_for('admin_dashboard')) # Replace with your admin route name
                
                # Check if rule exists
                rule = ModalityCompressionRule.query.filter_by(modality_name=modality_name).first()
                if not rule:
                    rule = ModalityCompressionRule(modality_name=modality_name)
                    db.session.add(rule)
                    
                rule.compression_type = comp_type
                rule.compression_level = max(10, min(100, comp_level)) # Clamp 10-100
                
                db.session.commit()
                flash(f"Compression rule updated for {modality_name}", "success")
                
            elif action == 'delete':
                rule_id = request.form.get('rule_id')
                rule = db.session.get(ModalityCompressionRule, rule_id)
                if rule:
                    db.session.delete(rule)
                    db.session.commit()
                    flash(f"Compression rule deleted.", "success")
                    
        except Exception as e:
            db.session.rollback()
            log.error(f"Failed to update compression rule: {e}")
            flash(f"Database error: {e}", "error")

    # Redirect back to the admin page
    return redirect('/admin') # Replace '/admin' with your actual route if different

@app.route('/api/admin/compression_rules/<int:rule_id>', methods=['DELETE'])
def delete_compression_rule(rule_id):
    with db_lock:
        rule = db.session.get(ModalityCompressionRule, rule_id)
        if rule:
            db.session.delete(rule)
            db.session.commit()
            return jsonify({"success": True})
        return jsonify({"success": False, "error": "Rule not found"}), 404
#-----------#---------------------#----------------------------------#-----------------------------------------


#helper functions



import ctypes

def enforce_single_instance():
    """
    Asks Windows for a unique lock. If another .exe already holds it, 
    this function forces the new instance to silently exit.
    """
    # A unique string identifier for your specific application
    mutex_name = "Global\\IcareEnterpriseDicomGateway_Mutex"
    
    # Ask Windows to create or find this Mutex
    mutex = ctypes.windll.kernel32.CreateMutexW(None, False, mutex_name)
    last_error = ctypes.windll.kernel32.GetLastError()
    
    # Error Code 183 means "ERROR_ALREADY_EXISTS"
    if last_error == 183:
        print("Application is already running. Exiting second instance.")
        sys.exit(0)
        
    # We must return the mutex to keep it alive in memory!
    return mutex
# ==========================================
# HELPER FUNCTIONS & GRACEFUL SHUTDOWN
# ==========================================
import socket

is_shutting_down = False
worker_thread = None
active_workers = []

def graceful_shutdown():
    """The Fire Chief: Orchestrates the safe shutdown of the entire hospital."""
    global is_shutting_down
    
    if is_shutting_down:
        return 
        
    is_shutting_down = True
    log.info("Graceful shutdown initiated")
    
    try:
        stop_dicom_server() 
    except Exception as e:
        log.error(f"Failed to stop DICOM server cleanly: {e}")
    
    from core.event_bus import db_write_queue, DBWriteEvent
    items_left = db_write_queue.qsize()
    log.info(f"Dispatching poison pill. {items_left} items remaining in queue.")
    db_write_queue.put(DBWriteEvent(action="SHUTDOWN", payload={}))
    
    # Also set the flag if the worker supports it
    if hasattr(worker_thread, 'shutdown'):
        worker_thread.shutdown()
        
    for w in active_workers:
        if hasattr(w, 'shutdown'):
            w.shutdown()
    
    # SAFEGUARD: Timed wait instead of infinite hang
    if worker_thread and worker_thread.is_alive():
        log.info("Waiting up to 10s for database worker to drain queue...")
        worker_thread.join(timeout=10.0) 
        
        if worker_thread.is_alive():
            log.critical("Database worker thread timed out! Forcing exit.")
        else:
            log.info("Database worker thread successfully terminated")
    
    log.info("All systems safe, initiating final exit.")
    # Notice: NO os._exit(0). We let the script end naturally so the OS window closes cleanly.


# def on_webview_closed():
#     """Triggered automatically when the user clicks the 'X' on the window."""
#     log.info("UI Window closed by user. Initiating teardown sequence...")
#     graceful_shutdown()

# ==========================================
# THREAD MANAGER
# ==========================================
from waitress import serve
def start_enterprise_pipeline(app):
    global worker_thread
    
    # In the new architecture, we'll just check if the global flag is set
    # Or rely on the fact that this is called once.
    if worker_thread and hasattr(worker_thread, 'is_alive') and worker_thread.is_alive():
        log.info("Enterprise Pipeline is already running. Skipping boot sequence.")
        return True

    log.info("✅ Booting Enterprise Pipeline...")
    from mapper import sync_rules_to_database
    
    def background_rule_sync():
        with app.app_context():
            try:
                sync_rules_to_database()
            except Exception as e:
                log.error(f"Background rule sync failed at boot: {e}")
                
    threading.Thread(target=background_rule_sync, daemon=True, name="RuleSyncThread").start()
    
    def recover_pipeline_state():
        from core.database import db, ImageInstance, Series, PipelineStatus
        from core.event_bus import compression_queue, CompressionTaskEvent
        with app.app_context():
            try:
                # Reset stuck PROCESSING statuses
                stuck_images = ImageInstance.query.filter_by(compression_status=PipelineStatus.PROCESSING).all()
                for img in stuck_images:
                    img.compression_status = PipelineStatus.PENDING
                
                if stuck_images:
                    db.session.commit()
                
                # Requeue all PENDING images so the compressor resumes
                pending_images = ImageInstance.query.join(Series).filter(
                    ImageInstance.compression_status == PipelineStatus.PENDING
                ).all()
                for img in pending_images:
                    study_uid = img.series.study_uid
                    compression_queue.put(CompressionTaskEvent(
                        file_path=img.file_path,
                        study_uid=study_uid,
                        sop_uid=img.sop_uid
                    ))
                    
                if stuck_images or pending_images:
                    log.info(f"Pipeline Recovery: Reset {len(stuck_images)} stuck images, Queued {len(pending_images)} pending compressions.")
            except Exception as e:
                log.error(f"Pipeline Recovery failed: {e}")
                
    threading.Thread(target=recover_pipeline_state, daemon=True, name="PipelineRecoveryThread").start()
  
    # --- CQRS Database Writer Service ---
    from core.db_writer import DatabaseWriterService
    worker_thread = DatabaseWriterService(app)
    worker_thread.start()
    log.info("DatabaseWriterService thread initialized")
    
    # --- Workers ---
    from workers.compression_worker import CompressionWorkerService
    from workers.zip_worker import ZipWorkerService
    from workers.upload_worker import UploadWorkerService, UploadWatchdogService
    from ingestion.dicom_receiver import start_dicom_server, dicom_watchdog_loop
    
    for _ in range(Config.COMPRESSION_WORKERS):
        w = CompressionWorkerService(app)
        w.start()
        active_workers.append(w)
        
    w = ZipWorkerService(app)
    w.start()
    active_workers.append(w)
    
    for _ in range(Config.UPLOAD_WORKERS):
        w = UploadWorkerService(app)
        w.start()
        active_workers.append(w)
        
    w = UploadWatchdogService(app)
    w.start()
    active_workers.append(w)

    from workers.api_sync_worker import ApiSyncWorkerService
    from workers.maintenance_worker import MaintenanceWorkerService
    
    w = ApiSyncWorkerService(app)
    w.start()
    active_workers.append(w)
    
    w = MaintenanceWorkerService(app)
    w.start()
    active_workers.append(w)
    
    log.info("API Sync and Maintenance worker threads started")
    
    threading.Thread(target=start_dicom_server, args=(app,), daemon=True).start()
    log.info("DICOM Listener started in background")
    
    threading.Thread(target=dicom_watchdog_loop, args=(app,), daemon=True).start()
    
    return True

def enable_legacy_safeguards():
    try:
        p = psutil.Process(os.getpid())
        if os.name == 'nt': 
            p.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)
        else: 
            p.nice(10) 
        log.info("Background priority engaged: Legacy system UI will remain responsive.")
    except Exception as e:
        log.warning(f"Could not adjust OS priority: {e}")


def run_flask(port):
    log.info(f"Starting Flask Web Interface on port {port}")
    # use_reloader=False is MANDATORY for .exe
    from waitress import serve
    serve(app, host='127.0.0.1', port=port, threads=Config.WAITRESS_THREADS, clear_untrusted_proxy_headers=True)
# ==========================================
# MAIN EXECUTION
# ==========================================
if __name__ == '__main__':
    # --- MULTIPROCESSING FIX FOR .EXE COMPILATION ---
    import multiprocessing
    multiprocessing.freeze_support()
    _global_mutex = enforce_single_instance()
    log.info(f"Enterprise System Lock Acquired: {_global_mutex}")
    # =================================================================
    # FORCE LOAD .ENV FOR COMPILED EXECUTABLES
    # =================================================================
    from dotenv import load_dotenv
    import sys
    # Find exactly where the .exe is physically located
    base_dir = os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else os.path.abspath(os.path.dirname(__file__))
    
    # Force load the .env file from that specific folder
    env_path = os.path.join(base_dir, '.env')
    load_dotenv(env_path)
    
    # GRAB THE LIVE PORT JIT (Just-In-Time)
    LIVE_UI_PORT = int(os.environ.get('WEB_UI_PORT', 5000))
    Config.validate()
    # =================================================================
    with app.app_context():
        db.create_all() 
        
    os.makedirs(Config.INCOMING_FOLDER, exist_ok=True)
    os.makedirs(Config.BACKUP_FOLDER, exist_ok=True)

    log.info("Verifying Gateway Identity...")
    auth_status = authenticate_gateway_on_boot(app)
    
    if auth_status is True:
        log.info("✅ Auth successful. Proceeding to pipeline boot...")
        start_enterprise_pipeline(app) 
    elif auth_status == "NEEDS_SETUP":
        log.warning("⚙️ SETUP MODE ACTIVE: Background processors and DICOM Server are PAUSED.")
    else:
        log.critical("❌ AUTH FAILURE: The Central Server rejected the credentials.")

    # Apply priority safeguards
    enable_legacy_safeguards()

    # Start Flask in background
    threading.Thread(target=run_flask, args=(LIVE_UI_PORT,), daemon=True).start()

    # --- THE ULTRA-FAST PORT CHECKER ---
    log.info("Waiting for UI Server to bind...")
    import socket
    port_bound = False
    for _ in range(20):  # Check every 100ms for up to 2 seconds
        try:
            with socket.create_connection(('127.0.0.1', LIVE_UI_PORT), timeout=0.1):
                port_bound = True
                break
        except OSError:
            time.sleep(0.1)

    if not port_bound:
        log.warning("Port check timed out, but forcing UI window to open anyway!")

    class WebAPI:
        def confirm_quit(self):
            log.info("🚪 User confirmed exit via custom UI. Triggering graceful shutdown...")
            graceful_shutdown()
            window.allow_close = True
            window.destroy()
            return True

    api = WebAPI()

    # Create UI Window
    window = webview.create_window(
        "Powered BY Icare Infosystem Pvt. Ltd.",
       f"http://127.0.0.1:{LIVE_UI_PORT}",
        width=1400,
        height=900,
        js_api=api
    )
    
    # 1. Define the interception logic INSIDE the main block so it has access to 'window'
    def confirm_exit():
        # If the API triggered the quit, we allow it
        if getattr(window, 'allow_close', False):
            return True
            
        # Otherwise, intercept the close and show the custom HTML modal in the UI
        import threading
        threading.Timer(0.1, lambda: window.evaluate_js('if (window.showCustomExitModal) { window.showCustomExitModal(); } else { pywebview.api.confirm_quit(); }')).start()
        return False

    # 2. Hook to the 'closing' event (NOT 'closed'!)
    window.events.closing += confirm_exit

    # =================================================================
    # --- CLOSE THE SPLASH SCREEN ---pyinstaller
    # =================================================================
    try:
        import pyi_splash
        # Update the text on the splash screen (optional)
        pyi_splash.update_text('UI Ready! Launching...')
        # Close the splash screen so the main window can appear
        pyi_splash.close()
    except ImportError:
        # If we are running in dev mode (python app.py), pyi_splash doesn't exist, so we just ignore it.
        pass

    try:
        # Blocks the main thread until the user closes the window
        webview.start(gui='edgechromium', private_mode=False)
    except KeyboardInterrupt:
        log.warning("Terminal CTRL+C detected. Forcing closure...")
        graceful_shutdown()

    # =================================================================
    # --- CLOSE THE NUITKA SPLASH SCREEN ---
    # =================================================================
    # if "NUITKA_ONEFILE_PARENT" in os.environ:
    #     import tempfile
    #     splash_filename = os.path.join(
    #         tempfile.gettempdir(),
    #         "onefile_%d_splash_feedback.tmp" % int(os.environ["NUITKA_ONEFILE_PARENT"]),
    #     )
    #     if os.path.exists(splash_filename):
    #         os.unlink(splash_filename)

    # try:
    #     # Blocks the main thread until the user closes the window
    #     webview.start()
    # except KeyboardInterrupt:
    #     log.warning("Terminal CTRL+C detected. Forcing closure...")
    #     graceful_shutdown()
