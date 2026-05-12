import os
import time
import queue
import shutil
import hashlib
import threading
from datetime import datetime, timedelta
from sqlalchemy.exc import IntegrityError
import zipfile
from config import Config
from logger import get_enterprise_logger
from zipper import create_chunked_zips

# Import the DB, Locks, Queue, and Models from our new Data Layer
from database import db, db_lock, file_lock, db_queue, PatientStudy, ImageInstance, AuditLog, PipelineStatus, Series,  ModalityZipRule

# Import the logic from our new Math Layer
from compressor import compress_single_dicom
# from uploader import upload_zip_with_backoff
from activity_tracker import tracker
from systemProfile import SystemProfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from mapper import get_official_description
from upload_bridge import upload_zip_with_backoff
import enterprise_uploader

log = get_enterprise_logger("Processors")
import threading
active_uploads_lock = threading.Lock()
active_uploads_set = set()

upload_queue = queue.Queue(maxsize=2000)
# --- PERFORMANCE METRICS ---
class PerformanceMetrics:
    def __init__(self):
        self.lock = threading.Lock()
        self.count = 0
        self.total_time = 0.0

    def add_record(self, duration):
        with self.lock:
            self.count += 1
            self.total_time += duration

    def flush(self):
        with self.lock:
            c, t = self.count, self.total_time
            self.count, self.total_time = 0, 0.0
        return c, t

metrics = PerformanceMetrics()

def metrics_logger_loop():
    while True:
        time.sleep(60)
        count, total_time = metrics.flush()
        if count > 0:
            avg_time = total_time / count
            log.info("System Performance", extra={"images_per_min": count, "avg_time_sec": round(avg_time, 4)})



# --- DB WORKER LOOP ---
def db_worker_loop(app):
    processed_count = 0
    with app.app_context():
        while True:
            try: 
                payload = db_queue.get(timeout=2.0)
            except queue.Empty: 
                continue

            if payload == "SHUTDOWN":
                log.info("Database worker clocking out safely")
                db_queue.task_done()
                break

            items_left = db_queue.qsize()
            if items_left % 10 == 0:
                tracker.set_activity(f"Receiving Network Data ({items_left} items in queue)...")

            # Extract data from the new dictionary payload
            s_uid = payload['study_uid']
            series_uid = payload['series_uid']
            sop_uid = payload['sop_uid']
            temp_path = payload['temp_incoming']
            final_path = payload['final_incoming']
            start_time = payload['start_time']
            
            file_renamed = False 
            
            # --- NEW: Fetch Official Mapping (CASE 1) ---
            raw_desc = payload['study_description']
            modality = payload['modality']
            official_desc = get_official_description(raw_desc, modality)
                        
            # if official_desc:
            #                 log.info(f"Auto-Mapped Description: '{raw_desc}' -> '{official_desc}'")
            # else:
            #                 log.info(f"No Auto-Map found for: '{raw_desc}'")
                        # --------------------------------------------
            try:
                with db_lock: 
                    # 1. STUDY LEVEL UPSERT
                    study = PatientStudy.query.filter_by(study_uid=s_uid).first()
                    if not study:
                        study = PatientStudy(
                            study_uid=s_uid, 
                            patient_id=payload['patient_id'], 
                            patient_name=payload['patient_name'],
                            patient_age=payload['patient_age'], 
                            patient_sex=payload['patient_sex'], 
                            patient_birth_date=payload['patient_birth_date'], # NEW
                            modality=payload['modality'],
                            study_date=payload['study_date'], 
                            study_description=raw_desc,            # The raw machine string
                            outgoing_description=official_desc,
                            
                            institution_name=payload['institution_name'],
                            ref_physician_name=payload['ref_physician_name'],
                            accession_number=payload['accession_number'], # NEW
                            manufacturer=payload['manufacturer'], # NEW
                            manufacturer_model_name=payload['manufacturer_model_name'], # NEW
                            image_count=0
                        )
                        db.session.add(study)
                        db.session.add(AuditLog(action_type='STUDY_CREATED', study_uid=s_uid, sop_uid=sop_uid, source='NETWORK'))
                    else:
                        study.last_received = datetime.now()

                    # 2. SERIES LEVEL UPSERT
                    series = Series.query.filter_by(series_uid=series_uid).first()
                    if not series:
                        series = Series(
                            series_uid=series_uid,
                            study_uid=s_uid,
                            series_number=payload['series_number'],
                            series_description=payload['series_description']
                        )
                        db.session.add(series)

                  
                    # 3. IMAGE LEVEL UPSERT
                    image = ImageInstance.query.filter_by(sop_uid=sop_uid).first()
                    if not image:
                        new_image = ImageInstance(
                            sop_uid=sop_uid, 
                            series_uid=series_uid,
                            instance_number=payload['instance_number'],
                            file_path=final_path,
                            window_width=payload['window_width'], 
                            window_center=payload['window_center'], 
                            slice_thickness=payload['slice_thickness'], 
                            acquisition_time=payload['acquisition_time'], 
                            slice_location=payload['slice_location'],
                            
                            # --- ADD THE NEW FIELDS HERE ---
                            columns=payload['columns'],
                            rows=payload['rows'],
                            photometric_interpretation=payload['photometric_interpretation'],
                            bits_allocated=payload['bits_allocated'],
                            bits_stored=payload['bits_stored'],
                            pixel_representation=payload['pixel_representation'],
                            samples_per_pixel=payload['samples_per_pixel'],
                            pixel_spacing=payload['pixel_spacing'],
                            image_orientation_patient=payload['image_orientation_patient'],
                            image_position_patient=payload['image_position_patient'],
                            frame_of_reference_uid=payload['frame_of_reference_uid'],
                            image_type=payload['image_type'],
                            sop_class_uid=payload['sop_class_uid'],
                            high_bit=payload['high_bit']
                        )
                        db.session.add(new_image)
                        study.image_count += 1
                        db.session.add(AuditLog(action_type='IMAGE_RECEIVED', study_uid=s_uid, sop_uid=sop_uid, source='NETWORK'))

                    db.session.flush()

                    with file_lock:
                        if os.path.exists(temp_path) and not os.path.exists(final_path):
                            os.rename(temp_path, final_path)
                            file_renamed = True

                    db.session.commit()
                    metrics.add_record(time.time() - start_time)
                    processed_count += 1

            except IntegrityError:
                # 1. DEDUPLICATION: Safely ignore duplicate files without crashing or flooding logs
                db.session.rollback()
                log.info(f"Deduplication: SOP {sop_uid} already exists. Skipping.")
                if not file_renamed and os.path.exists(temp_path):
                    try: os.remove(temp_path)
                    except OSError: pass
                processed_count += 1

            except Exception as e:
                # 2. CRITICAL FIX: Catch real errors so the thread doesn't die!
                db.session.rollback() 
                log.error(f"Transaction failed for {sop_uid}: {e}", exc_info=True)
                
                # Clean up everything to prevent "Ghost" files taking up space
                if file_renamed and os.path.exists(final_path):
                    try: os.remove(final_path)
                    except OSError: pass
                if not file_renamed and os.path.exists(temp_path):
                    try: os.remove(temp_path)
                    except OSError: pass

            finally:
                # 4. THE IRON GUARANTEE
                db_queue.task_done()
                
                # --- RESTORED: Queue metrics logging ---
                if processed_count % 100 == 0 and processed_count > 0:
                    items_left = db_queue.qsize()
                    log.info("Database worker metrics update", extra={
                        "processed_count": processed_count, 
                        "items_waiting": items_left
                    })

# --- JANITOR LOOP ---
def cleanup_empty_folders(base_dir):
    for dirpath, dirnames, filenames in os.walk(base_dir, topdown=False):
        if not os.listdir(dirpath): 
            try: os.rmdir(dirpath)
            except OSError: pass

from database import get_system_setting

def get_dir_age_days(path: str) -> float:
    """Returns how many days old a file or folder is based on its modification time."""
    try:
        mtime = os.path.getmtime(path)
        return (time.time() - mtime) / (24 * 3600)
    except OSError:
        return 0

def database_janitor_loop(app):
    log.info("DB Janitor initialized")
    while True:
        time.sleep(3600)  # Check every hour
        with app.app_context():
            db_days = int(get_system_setting('DB_RETENTION_DAYS', 30))
            cutoff = datetime.now() - timedelta(days=db_days)
            
            with db_lock:
                try:
                    expired = PatientStudy.query.filter(PatientStudy.last_received < cutoff).all()
                    for study in expired:
                        uid_copy = study.study_uid 
                        db.session.delete(study)
                        db.session.add(AuditLog(action_type='PURGED', study_uid=uid_copy, source='DB_JANITOR'))
                    if expired:
                        db.session.commit()
                        log.info(f"🗑️ DB Janitor purged {len(expired)} old database records.")
                except Exception as e:
                    db.session.rollback()
                    log.error(f"DB Janitor failed: {e}")

def backup_janitor_loop(app):
    log.info("Backup Janitor initialized")
    while True:
        time.sleep(3600)  # Check every hour
        backup_days = int(get_system_setting('BACKUP_RETENTION_DAYS', 60))
        
        with file_lock:
            try:
                if os.path.exists(Config.BACKUP_FOLDER):
                    for patient_folder in os.listdir(Config.BACKUP_FOLDER):
                        patient_path = os.path.join(Config.BACKUP_FOLDER, patient_folder)
                        if os.path.isdir(patient_path):
                            for study_folder in os.listdir(patient_path):
                                study_path = os.path.join(patient_path, study_folder)
                                if os.path.isdir(study_path) and get_dir_age_days(study_path) > backup_days:
                                    shutil.rmtree(study_path)
                                    log.info(f"🗑️ Backup Janitor purged old backup: {study_folder}")
                    cleanup_empty_folders(Config.BACKUP_FOLDER)
            except Exception as e:
                log.error(f"Backup Janitor failed: {e}")

def working_folder_janitor_loop(app):
    log.info("Working Folder Janitor initialized")
    outbound_base = getattr(Config, 'OUTBOUND_FOLDER', 'Outbound_Data')
    while True:
        time.sleep(3600)  # Check every hour
        work_days = int(get_system_setting('WORKING_RETENTION_DAYS', 7))
        
        with file_lock:
            try:
                # 1. Clean Incoming Folder
                if os.path.exists(Config.INCOMING_FOLDER):
                    for patient_folder in os.listdir(Config.INCOMING_FOLDER):
                        patient_path = os.path.join(Config.INCOMING_FOLDER, patient_folder)
                        if os.path.isdir(patient_path):
                            for study_folder in os.listdir(patient_path):
                                study_path = os.path.join(patient_path, study_folder)
                                if os.path.isdir(study_path) and get_dir_age_days(study_path) > work_days:
                                    shutil.rmtree(study_path)
                                    log.info(f"🗑️ Working Janitor purged old incoming data: {study_folder}")
                    cleanup_empty_folders(Config.INCOMING_FOLDER)

                # 2. Clean Outbound Data (ZIPs and Staging)
                if os.path.exists(outbound_base):
                    # Clean stray ZIP files, stranded .uploaded files, and broken .tmp files
                    for f in os.listdir(outbound_base):
                        f_path = os.path.join(outbound_base, f)
                        
                        # 🚨 THE FIX: Look for all 3 variations of outbound files
                        is_target_file = f.endswith(('.zip', '.zip.uploaded', '.zip.tmp'))
                        
                        if os.path.isfile(f_path) and is_target_file and get_dir_age_days(f_path) > work_days:
                            try:
                                os.remove(f_path)
                                log.info(f"🗑️ Working Janitor purged old outbound file: {f}")
                            except Exception as e:
                                log.error(f"Janitor could not delete {f}: {e}")
                    
                    # Clean staging folders
                    staging_dir = os.path.join(outbound_base, 'staging')
                    if os.path.exists(staging_dir):
                        for study_folder in os.listdir(staging_dir):
                            study_path = os.path.join(staging_dir, study_folder)
                            if os.path.isdir(study_path) and get_dir_age_days(study_path) > work_days:
                                shutil.rmtree(study_path)
                                log.info(f"🗑️ Working Janitor purged old staging data: {study_folder}")
            except Exception as e:
                log.error(f"Working Folder Janitor failed: {e}")

import concurrent.futures
import time
import os

def compression_loop(app):
    log.info("Continuous Compression Thread initialized.")
    outbound_base = getattr(Config, 'OUTBOUND_FOLDER', 'Outbound_Data')
    compressed_stage = os.path.join(outbound_base, 'staging')
    os.makedirs(compressed_stage, exist_ok=True)
    
    # 1. HARDWARE INTELLIGENCE
    workers = SystemProfile.get_optimal_threads(task_type="CPU")
    log.info(f"⚡ Compression Engine scaled to {workers} concurrent hardware threads.")
    
    # =======================================================================
    # 🚨 THE FIX: Thread-Safe Flask Context Wrapper
    # =======================================================================
    def run_compression_in_thread(input_path, target_path, modality, flask_app):
        """Infects the background CPU thread with the Flask context so it can read the DB."""
        with flask_app.app_context():
            from compressor import compress_single_dicom
            return compress_single_dicom(input_path, target_path, modality)

    while True:
        tasks = []
        with app.app_context():
            try:
                with db_lock:
                    # Query a batch of 50 to keep DB locks lightning fast
                    pending_images = ImageInstance.query.filter_by(
                        compression_status=PipelineStatus.PENDING
                    ).limit(50).all()
                    
                    if pending_images:
                        # Pre-calculate paths and mark PROCESSING instantly
                        for img in pending_images:
                            img.compression_status = PipelineStatus.PROCESSING
                            
                            series = Series.query.get(img.series_uid)
                            study = PatientStudy.query.get(series.study_uid)
                            
                            output_dir = os.path.join(compressed_stage, study.study_uid)
                            os.makedirs(output_dir, exist_ok=True)
                            target_path = os.path.join(output_dir, os.path.basename(img.file_path))
                            
                            tasks.append({
                                'sop_uid': img.sop_uid,
                                'input_path': img.file_path,
                                'target_path': target_path,
                                'modality': study.modality
                            })
                        db.session.commit()
            
            except Exception as e:
                db.session.rollback()
                log.error(f"Failed to query compression batch: {e}", exc_info=True)
                time.sleep(2) # Backoff if the DB is temporarily locked
                continue

        # =======================================================================
        # ⚡ SPEED UPGRADE: Dynamic Sleeping
        # =======================================================================
        if not tasks:
            time.sleep(1) # ONLY sleep if there is absolutely no work to do!
            continue

        # =======================================================================
        # 2. THE MULTITHREADED ENGINE
        # =======================================================================
        results = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            # Pass the tasks AND the main app object to our new thread wrapper
            future_to_sop = {
                executor.submit(
                    run_compression_in_thread, 
                    task['input_path'], 
                    task['target_path'], 
                    task['modality'],
                    app  # <-- Passing the Flask app context here
                ): task for task in tasks
            }
            
            for future in concurrent.futures.as_completed(future_to_sop):
                task_data = future_to_sop[future]
                try:
                    success = future.result()
                    results.append({
                        'sop_uid': task_data['sop_uid'],
                        'success': success,
                        'target_path': task_data['target_path']
                    })
                except Exception as e:
                    log.error(f"Thread crash on {os.path.basename(task_data['input_path'])}: {e}")
                    results.append({
                        'sop_uid': task_data['sop_uid'],
                        'success': False,
                        'target_path': None
                    })

        # =======================================================================
        # 3. BATCH DATABASE UPDATE
        # =======================================================================
        with app.app_context():
            with db_lock:
                try:
                    for res in results:
                        locked_img = ImageInstance.query.get(res['sop_uid'])
                        if locked_img: # Safety check
                            if res['success']:
                                locked_img.staged_file_path = res['target_path']
                                locked_img.compression_status = PipelineStatus.COMPLETED
                                locked_img.zip_status = PipelineStatus.PENDING # Handoff to Zipper
                            else:
                                locked_img.compression_status = PipelineStatus.FAILED
                    db.session.commit()
                except Exception as e:
                    db.session.rollback()
                    log.error(f"Failed to batch update compression results: {e}")
                    
                    # 🚨 THE FIX: If SQLite locks up, revert them to PENDING so they aren't orphaned!
                    try:
                        for res in results:
                            img = ImageInstance.query.get(res['sop_uid'])
                            if img:
                                img.compression_status = PipelineStatus.PENDING
                        db.session.commit()
                    except Exception:
                        db.session.rollback()


def zipping_loop(app):
    log.info("Continuous Zipping Thread initialized.")
    outbound_base = getattr(Config, 'OUTBOUND_FOLDER', 'Outbound_Data')
    
    while True:
        time.sleep(2)
        
        with app.app_context():
            try:
                cooldown_sec = int(get_system_setting('STUDY_COOLDOWN_SECONDS', 15))
                flush_threshold = datetime.now() - timedelta(seconds=cooldown_sec)
                
                # 🚨 THE FIX: Store the batches to be zipped in RAM, OUTSIDE the DB Lock
                work_batches = []
                
                with db_lock:
                    # Find studies with AT LEAST ONE image waiting to be zipped
                    active_studies = PatientStudy.query.join(Series).join(ImageInstance).filter(
                        ImageInstance.zip_status == PipelineStatus.PENDING
                    ).distinct().all()

                    for study in active_studies:
                        # 1. Fetch Modality Zip Rule
                        zip_rule = ModalityZipRule.query.filter_by(modality_name=study.modality).first()
                        batch_limit = zip_rule.max_files_per_zip if zip_rule else 50
                        
                        # 2. Gather Ready Images
                        ready_images = ImageInstance.query.join(Series).filter(
                            Series.study_uid == study.study_uid,
                            ImageInstance.zip_status == PipelineStatus.PENDING,
                            ImageInstance.compression_status == PipelineStatus.COMPLETED
                        ).order_by(ImageInstance.received_at).all()
                        
                        if not ready_images:
                            continue

                        # Are there images STILL waiting to be compressed?
                        uncompressed_count = ImageInstance.query.join(Series).filter(
                            Series.study_uid == study.study_uid,
                            ImageInstance.compression_status.in_([PipelineStatus.PENDING, PipelineStatus.PROCESSING])
                        ).count()

                        # 3. The Trigger Logic
                        is_full_batch = len(ready_images) >= batch_limit
                        is_network_quiet = study.last_received < flush_threshold
                        is_straggler_flush = is_network_quiet and (uncompressed_count == 0)
                        
                        if is_full_batch or is_straggler_flush:
                            batch_to_zip = ready_images[:batch_limit]
                            
                            # Mark PROCESSING instantly so no other loop touches them
                            staged_paths = []
                            img_uids = []
                            for img in batch_to_zip:
                                img.zip_status = PipelineStatus.PROCESSING
                                if img.staged_file_path and os.path.exists(img.staged_file_path):
                                    staged_paths.append(img.staged_file_path)
                                img_uids.append(img.sop_uid)
                            
                            db.session.commit()
                            
                            # Save the required data to process later
                            work_batches.append({
                                'study_uid': study.study_uid,
                                'modality': study.modality,
                                'staged_paths': staged_paths,
                                'img_uids': img_uids
                            })

                # =========================================================================
                # 🚀 HEAVY LIFTING (ZIPPING & QUEUING) NOW HAPPENS OUTSIDE THE DB LOCK
                # =========================================================================
                for batch in work_batches:
                    batch_count = len(batch['img_uids'])
                    timestamp = int(time.time())
                    zip_filename = f"{batch['modality']}_{batch['study_uid']}_contains_{batch_count}_images_{timestamp}.zip"
                    zip_filepath = os.path.join(outbound_base, zip_filename)
                    
                    success = False
                    try:
                        # 4. Zip the files (This takes high CPU/Disk IO and no longer freezes the DB)
                        with zipfile.ZipFile(zip_filepath, 'w', zipfile.ZIP_DEFLATED) as zf:
                            for file_path in batch['staged_paths']:
                                zf.write(file_path, os.path.basename(file_path))
                        
                        # 5. Handoff to Memory Queue
                        # If the queue is full, this gracefully pauses just the zipper thread!
                        upload_payload = {
                            'zip_path': zip_filepath,
                            'study_uid': batch['study_uid'],
                            'image_count': batch_count
                        }
                        upload_queue.put(upload_payload)
                        success = True
                        log.info(f"Packaged chunk of {batch_count} images for {batch['study_uid']}")

                        for file_path in batch['staged_paths']:
                            try:
                                if os.path.exists(file_path):
                                    os.remove(file_path)
                            except OSError as cleanup_err:
                                log.warning(f"Could not delete staged file {file_path}: {cleanup_err}")
                        
                    except Exception as zip_err:
                        log.error(f"Failed to create ZIP chunk for {batch['study_uid']}: {zip_err}", exc_info=True)
                        
                    # 6. Briefly re-acquire DB Lock just to stamp the final status
                    with db_lock:
                        imgs = ImageInstance.query.filter(ImageInstance.sop_uid.in_(batch['img_uids'])).all()
                        for img in imgs:
                            img.zip_status = PipelineStatus.COMPLETED if success else PipelineStatus.FAILED
                        db.session.commit()

            except Exception as e:
                db.session.rollback()
                log.error(f"Crash in zipping loop: {e}", exc_info=True)






def upload_loop(app):
    log.info("Continuous Upload Thread initialized.")
    
    while True:
        # 1. Grab item from queue (Safe blocking)
        try:
            payload = upload_queue.get(block=True, timeout=5)
        except queue.Empty:
            continue


        if payload == "SHUTDOWN":
            log.info("Upload thread received poison pill. Shutting down gracefully.")
            upload_queue.task_done()
            break    
        # 2. MASTER TRY/FINALLY: Guarantees task_done() fires exactly once per item
        try:
            zip_path = payload['zip_path']
            study_uid = payload['study_uid']
            chunk_count = payload['image_count']

            if not os.path.exists(zip_path):
                continue
            
            # Concurrency Collision Shield
            with active_uploads_lock:
                if zip_path in active_uploads_set:
                    continue 
                active_uploads_set.add(zip_path) 

            try:
                should_upload = False
                with app.app_context():
                    study = PatientStudy.query.get(study_uid)
                    if not study:
                        continue
                    
                    from database import InstitutionMapping, PipelineStatus
                    mapping = InstitutionMapping.query.filter_by(dicom_institution_name=study.institution_name).first()
                    
                    if not mapping or not mapping.center:
                        # 1. The center is missing! Block the upload.
                        if study.remote_upload_id != "UNMAPPED_CENTER":
                            study.remote_upload_id = "UNMAPPED_CENTER"
                            study.upload_status = PipelineStatus.FAILED
                            db.session.commit()
                            
                        # 2. Requeue the file to try again later
                        def delayed_requeue():
                            upload_queue.put({'zip_path': zip_path, 'study_uid': study_uid, 'image_count': chunk_count})
                        threading.Timer(10.0, delayed_requeue).start()
                    else:
                        # Center IS mapped! Blast the file to the cloud immediately.
                        should_upload = True

                if not should_upload:
                    continue # Drops out to the Finally blocks

                # =============================================================
                # UPLOAD TO CLOUD
                # =============================================================

                
                log.info(f"Streaming ZIP chunk to cloud...", extra={"zip_file": os.path.basename(zip_path)})
                
                with app.app_context():
                    from database import get_system_setting
                    fresh_url = get_system_setting('TUS_API_URL', 'http://127.0.0.1:1080/files/')
                    fresh_user = get_system_setting('TUS_API_USER', 'unconfigured_user')
                    fresh_key = get_system_setting('TUS_API_KEY', '')
                
                success = upload_zip_with_backoff(zip_path, study_uid, fresh_url, fresh_user, fresh_key)
                
                # =============================================================
                # FRESH DATABASE SESSION FOR MATH CALCULATION
                # =============================================================
                live_uploaded = 0
                live_total = 0
                
                with app.app_context():
                    with db_lock:
                        study = PatientStudy.query.get(study_uid)
                        if not study:
                            continue

                        if success:
                            # Safe Math Increment
                            new_total = study.uploaded_image_count + chunk_count
                            study.uploaded_image_count = min(study.image_count, new_total)
                            
                            live_uploaded = study.uploaded_image_count
                            live_total = study.image_count
                            
                            if live_uploaded >= live_total:
                                study.upload_status = PipelineStatus.COMPLETED
                            else:
                                study.upload_status = PipelineStatus.PROCESSING
                                
                            db.session.commit()
                        else:
                            study.upload_status = PipelineStatus.FAILED
                            study.retry_count += 1
                            db.session.commit()
                            log.error(f"Network rejected chunk upload for {study_uid}")
                            
                            def delayed_requeue_fail():
                                upload_queue.put({'zip_path': zip_path, 'study_uid': study_uid, 'image_count': chunk_count})
                            threading.Timer(30.0, delayed_requeue_fail).start()
                    
                    # =============================================================
                    # API SYNC
                    # =============================================================
                    if success:
                        # 🚨 Only send the live count ping IF the API Sync Loop has created the study shell
                      with app.app_context():  
                        study_check = PatientStudy.query.get(study_uid)
                        if study_check and study_check.remote_upload_id == "SYNCED":
                            from api_client import sync_upload_count
                            sync_upload_count(study_uid, live_uploaded, live_total) 
                        
                        # Clean up ZIP from Hard Drive
                        if os.path.exists(zip_path):
                            safe_done_path = zip_path + ".uploaded"
                            try:
                                os.rename(zip_path, safe_done_path)
                                os.remove(safe_done_path)
                            except Exception:
                                pass

            except Exception as inner_e:
                log.error(f"Transaction failure in upload block for {study_uid}: {inner_e}", exc_info=True)
            finally:
                # Inner Cleanup: Release the collision shield so retries can work later
                with active_uploads_lock:
                    active_uploads_set.discard(zip_path)

        except Exception as fatal_e:
            log.critical(f"Catastrophic thread failure in upload loop: {fatal_e}", exc_info=True)
            time.sleep(5)
        finally:
            # Outer Cleanup: Master queue guarantee
            upload_queue.task_done()

def api_sync_loop(app):
    from api_client import send_study_to_api, sync_upload_count
    from database import PatientStudy, PipelineStatus, db, db_lock
    log.info("API Sync thread initialized and monitoring...")
    
    while True:
        with app.app_context():
            try:
                # =========================================================
                # LANE 1: NEW STUDIES (Initial Creation)
                # =========================================================
                # The upload_loop already verified the center mapping!
                pending_studies = PatientStudy.query.filter(
                    PatientStudy.remote_upload_id == None,
                    PatientStudy.uploaded_image_count > 0  # Only sync if bytes actually reached the cloud
                ).all()

                for study in pending_studies:
                    # Extract variables safely before the commit
                    s_uid_safe = study.study_uid
                    uploaded_safe = study.uploaded_image_count
                    total_safe = study.image_count
                    
                    log.info(f"Triggering Initial Remote API Creation for study: {s_uid_safe}")
                    
                    if send_study_to_api(s_uid_safe, is_update=False):
                        study.remote_upload_id = "SYNCED" 
                        db.session.commit() 
                        
                        # =====================================================
                        # 🚨 THE FIX: Indent this inside the 'if' block!
                        # =====================================================
                        try:
                            sync_upload_count(s_uid_safe, uploaded_safe, total_safe)
                        except Exception as ping_err:
                            log.error(f"Failed to sync initial progress bar: {ping_err}")        

                # =========================================================
                # LANE 2: INCREMENTAL UPDATES (More images arrived)
                # =========================================================
                incremental_studies = PatientStudy.query.filter(
                    PatientStudy.remote_upload_id == "SYNCED",
                    PatientStudy.uploaded_image_count > PatientStudy.api_synced_image_count
                ).all()

                for study in incremental_studies:
                    send_study_to_api(study.study_uid, is_update=True)

            except Exception as e:
                db.session.rollback()
                log.error(f"Error in API Sync Loop: {e}", exc_info=True)
                
        time.sleep(5)

def reset_orphaned_tasks(app):
    """Finds interrupted tasks, heals corrupted counts, and reloads the memory queue."""
    with app.app_context():
        with db_lock:
            # 1. DATABASE SELF-HEALING: Fix Corrupted Double-Counts
            corrupt_studies = PatientStudy.query.filter(PatientStudy.uploaded_image_count > PatientStudy.image_count).all()
            for c in corrupt_studies:
                c.uploaded_image_count = c.image_count
                log.info(f"🧹 Sweeper: Repaired corrupted image count for {c.study_uid}")

            # 2. Reset processing statuses for Studies
            orphans = PatientStudy.query.filter(
                (PatientStudy.compression_status == PipelineStatus.PROCESSING) |
                (PatientStudy.zip_status == PipelineStatus.PROCESSING) |
                (PatientStudy.upload_status == PipelineStatus.PROCESSING)
            ).all()
            
            for orphan in orphans:
                if orphan.compression_status == PipelineStatus.PROCESSING:
                    orphan.compression_status = PipelineStatus.PENDING
                if orphan.zip_status == PipelineStatus.PROCESSING:
                    orphan.zip_status = PipelineStatus.PENDING
                if orphan.upload_status == PipelineStatus.PROCESSING:
                    orphan.upload_status = PipelineStatus.PENDING
                    
            # =================================================================
            # 🚨 THE FIX: Reset stuck INDIVIDUAL IMAGES (The Zombie Bug)
            # =================================================================
            stuck_images = ImageInstance.query.filter(
                (ImageInstance.compression_status == PipelineStatus.PROCESSING) |
                (ImageInstance.zip_status == PipelineStatus.PROCESSING)
            ).all()
            
            for img in stuck_images:
                if img.compression_status == PipelineStatus.PROCESSING:
                    img.compression_status = PipelineStatus.PENDING
                if img.zip_status == PipelineStatus.PROCESSING:
                    img.zip_status = PipelineStatus.PENDING
                    
            if orphans or corrupt_studies or stuck_images:
                db.session.commit()
                log.info(f"🧹 Sweeper: Reset {len(orphans)} studies and {len(stuck_images)} zombie images.")        

        # 3. RAM QUEUE RECOVERY: Load stranded ZIPs back into memory
        import glob
        from config import Config
        outbound_base = getattr(Config, 'OUTBOUND_FOLDER', 'Outbound_Data')
        orphaned_zips = glob.glob(os.path.join(outbound_base, "*.zip"))
        
        requeued = 0
        for zip_path in orphaned_zips:
            filename = os.path.basename(zip_path)
            if '_contains_' in filename:
                try:
                    # Safely parse: Modality_StudyUID_contains_Count_images_Timestamp.zip
                    parts = filename.split('_contains_')
                    uid_part = parts[0].split('_', 1)[1] 
                    chunk_count = int(parts[1].split('_images')[0])
                    
                    upload_queue.put({
                        'zip_path': zip_path,
                        'study_uid': uid_part,
                        'image_count': chunk_count
                    })
                    requeued += 1
                except Exception as e:
                    log.error(f"Sweeper could not parse zip file {filename}", exc_info=True)
        
        if requeued > 0:
            log.info(f"🧹 Sweeper: Recovered {requeued} stranded ZIP files into RAM Queue.")