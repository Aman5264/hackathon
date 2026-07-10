import os
import time
import shutil
import glob
import threading
from datetime import datetime, timedelta
from core.database import db, db_lock, PatientStudy, ImageInstance, AuditLog, PipelineStatus, get_system_setting
from logger import get_enterprise_logger
from config import Config

log = get_enterprise_logger("MaintenanceWorker")

def cleanup_empty_folders(base_dir):
    for dirpath, dirnames, filenames in os.walk(base_dir, topdown=False):
        if not os.listdir(dirpath): 
            try: os.rmdir(dirpath)
            except OSError: pass

def get_dir_age_days(path: str) -> float:
    try:
        mtime = os.path.getmtime(path)
        return (time.time() - mtime) / (24 * 3600)
    except OSError:
        return 0

class MaintenanceWorkerService(threading.Thread):
    def __init__(self, app):
        super().__init__(name="MaintenanceWorkerService", daemon=True)
        self.app = app
        self._shutdown_flag = threading.Event()

    def shutdown(self):
        self._shutdown_flag.set()

    def _database_janitor(self):
        log.info("DB Janitor initialized")
        while not self._shutdown_flag.is_set():
            with self.app.app_context():
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
                            log.info(f"DB Janitor purged {len(expired)} old database records.")
                    except Exception as e:
                        db.session.rollback()
                        log.error(f"DB Janitor failed: {e}")
            for _ in range(3600):
                if self._shutdown_flag.is_set(): break
                time.sleep(1)

    def _backup_janitor(self):
        log.info("Backup Janitor initialized")
        while not self._shutdown_flag.is_set():
            with self.app.app_context():
                backup_days = int(get_system_setting('BACKUP_RETENTION_DAYS', 60))
            try:
                if os.path.exists(Config.BACKUP_FOLDER):
                    for patient_folder in os.listdir(Config.BACKUP_FOLDER):
                        patient_path = os.path.join(Config.BACKUP_FOLDER, patient_folder)
                        if os.path.isdir(patient_path):
                            for study_folder in os.listdir(patient_path):
                                study_path = os.path.join(patient_path, study_folder)
                                if os.path.isdir(study_path) and get_dir_age_days(study_path) > backup_days:
                                    shutil.rmtree(study_path)
                                    log.info(f"Backup Janitor purged old backup: {study_folder}")
                    cleanup_empty_folders(Config.BACKUP_FOLDER)
            except Exception as e:
                log.error(f"Backup Janitor failed: {e}")
            for _ in range(3600):
                if self._shutdown_flag.is_set(): break
                time.sleep(1)

    def _working_folder_janitor(self):
        log.info("Working Folder Janitor initialized")
        outbound_base = getattr(Config, 'OUTGOING_FOLDER', 'data/outgoing')
        while not self._shutdown_flag.is_set():
            with self.app.app_context():
                work_days = int(get_system_setting('WORKING_RETENTION_DAYS', 7))
                try:
                    if os.path.exists(Config.INCOMING_FOLDER):
                        for patient_folder in os.listdir(Config.INCOMING_FOLDER):
                            patient_path = os.path.join(Config.INCOMING_FOLDER, patient_folder)
                            if os.path.isdir(patient_path):
                                for study_folder in os.listdir(patient_path):
                                    study_path = os.path.join(patient_path, study_folder)
                                    if os.path.isdir(study_path) and get_dir_age_days(study_path) > work_days:
                                        shutil.rmtree(study_path)
                                        log.info(f"Working Janitor purged old incoming data: {study_folder}")
                        cleanup_empty_folders(Config.INCOMING_FOLDER)

                    if os.path.exists(outbound_base):
                        for f in os.listdir(outbound_base):
                            f_path = os.path.join(outbound_base, f)
                            
                            # --- ANTI-ZOMBIE SWEEPER ---
                            if f.endswith('.deleted'):
                                try:
                                    zip_file = f_path[:-8] # remove .deleted
                                    if os.path.exists(zip_file):
                                        os.remove(zip_file)
                                        log.info(f"Swept zombie zip: {os.path.basename(zip_file)}")
                                    os.remove(f_path)
                                except Exception:
                                    pass
                                continue

                            is_target_file = f.endswith(('.zip', '.zip.uploaded', '.zip.tmp', '.zip.failed', '.zip.streaming'))
                            if os.path.isfile(f_path) and is_target_file and get_dir_age_days(f_path) > work_days:
                                try:
                                    os.remove(f_path)
                                    log.info(f"Working Janitor purged old outbound file: {f}")
                                except Exception as e:
                                    log.error(f"Janitor could not delete {f}: {e}")
                        
                        staging_dir = os.path.join(outbound_base, 'staging')
                        if os.path.exists(staging_dir):
                            for study_folder in os.listdir(staging_dir):
                                study_path = os.path.join(staging_dir, study_folder)
                                if os.path.isdir(study_path) and get_dir_age_days(study_path) > work_days:
                                    shutil.rmtree(study_path)
                                    log.info(f"Working Janitor purged old staging data: {study_folder}")
                except Exception as e:
                    log.error(f"Working Folder Janitor failed: {e}")
            for _ in range(3600):
                if self._shutdown_flag.is_set(): break
                time.sleep(1)

    def _reset_orphaned_tasks(self):
        from core.event_bus import compression_queue, CompressionTaskEvent, upload_queue
        with self.app.app_context():
            with db_lock:
                corrupt_studies = PatientStudy.query.filter(PatientStudy.uploaded_image_count > PatientStudy.image_count).all()
                for c in corrupt_studies:
                    c.uploaded_image_count = c.image_count
                    log.info(f"Sweeper: Repaired corrupted image count for {c.study_uid}")

                orphans = PatientStudy.query.filter(
                    (PatientStudy.compression_status == PipelineStatus.PROCESSING) |
                    (PatientStudy.zip_status == PipelineStatus.PROCESSING) |
                    (PatientStudy.upload_status == PipelineStatus.PROCESSING)
                ).all()
                for orphan in orphans:
                    if orphan.compression_status == PipelineStatus.PROCESSING: orphan.compression_status = PipelineStatus.PENDING
                    if orphan.zip_status == PipelineStatus.PROCESSING: orphan.zip_status = PipelineStatus.PENDING
                    if orphan.upload_status == PipelineStatus.PROCESSING: orphan.upload_status = PipelineStatus.PENDING
                        
                stuck_images = ImageInstance.query.filter(
                    (ImageInstance.compression_status == PipelineStatus.PROCESSING) |
                    (ImageInstance.zip_status == PipelineStatus.PROCESSING)
                ).all()
                for img in stuck_images:
                    if img.compression_status == PipelineStatus.PROCESSING: img.compression_status = PipelineStatus.PENDING
                    if img.zip_status == PipelineStatus.PROCESSING: img.zip_status = PipelineStatus.PENDING
                        
                if orphans or corrupt_studies or stuck_images:
                    db.session.commit()
                    log.info(f"Sweeper: Reset {len(orphans)} studies and {len(stuck_images)} zombie images.")        

                stranded_uncompressed = ImageInstance.query.filter(ImageInstance.compression_status == PipelineStatus.PENDING).all()
                requeued_comp = 0
                for img in stranded_uncompressed:
                    if img.file_path and os.path.exists(img.file_path):
                        from core.database import Series
                        series = db.session.get(Series, img.series_uid)
                        if series:
                            compression_queue.put(CompressionTaskEvent(
                                sop_uid=img.sop_uid,
                                study_uid=series.study_uid,
                                file_path=img.file_path
                            ))
                            requeued_comp += 1
                if requeued_comp > 0: log.info(f"Sweeper: Recovered {requeued_comp} uncompressed images into compression queue.")

            outbound_base = getattr(Config, 'OUTGOING_FOLDER', 'data/outgoing')
            orphaned_zips = glob.glob(os.path.join(outbound_base, "*.zip"))
            requeued = 0
            for zip_path in orphaned_zips:
                filename = os.path.basename(zip_path)
                if '_contains_' in filename:
                    try:
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
            if requeued > 0: log.info(f"Sweeper: Recovered {requeued} stranded ZIP files into RAM Queue.")

    def run(self):
        log.info("Maintenance worker started")
        try:
            self._reset_orphaned_tasks()
        except Exception as e:
            log.error(f"Failed to reset orphaned tasks: {e}", exc_info=True)
        threading.Thread(target=self._database_janitor, daemon=True).start()
        threading.Thread(target=self._backup_janitor, daemon=True).start()
        threading.Thread(target=self._working_folder_janitor, daemon=True).start()
        
        while not self._shutdown_flag.is_set():
            time.sleep(1)
