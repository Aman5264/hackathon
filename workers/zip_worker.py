import os
import zipfile
import threading
import queue
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from flask import Flask

from core.event_bus import zip_queue, upload_queue, db_write_queue, DBWriteEvent, ZipTaskEvent, UploadTaskEvent
from core.state_machine import PipelineStatus
from logger import get_enterprise_logger
from config import Config
from core.database import db, PatientStudy, Series, ImageInstance, ModalityZipRule, get_system_setting

log = get_enterprise_logger("ZipWorker")

class ZipWorkerService(threading.Thread):
    def __init__(self, app: Flask):
        super().__init__(name="ZipWorker", daemon=True)
        self.app = app
        self._shutdown_flag = threading.Event()
        self._pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="ZipWorkerPool")

    def shutdown(self):
        self._shutdown_flag.set()
        self._pool.shutdown(wait=False)

    def run(self):
        log.info("ZipWorkerService started. Listening for tasks.")
        while not self._shutdown_flag.is_set():
            try:
                with self.app.app_context():
                    self._safe_run()
            except Exception as e:
                log.critical(f"FATAL THREAD CRASH in ZipWorker. Restarting in 5s. Error: {e}", exc_info=True)
                time.sleep(5)

    def _safe_run(self):
        while not self._shutdown_flag.is_set():
            try:
                # 1. Process tasks from queue
                try:
                    event = zip_queue.get(timeout=2.0)
                    if getattr(event, 'action', '') == "SHUTDOWN":
                        log.info("ZipWorkerService shutting down safely.")
                        zip_queue.task_done()
                        break
                    
                    if isinstance(event, ZipTaskEvent):
                        self._process_study(event.study_uid)
                    zip_queue.task_done()
                except queue.Empty:
                    pass

                # 2. Check for stragglers
                self._check_stragglers()

            except Exception as e:
                log.error(f"Error in ZipWorker loop: {e}", exc_info=True)
                time.sleep(1.0)

    def _process_study(self, study_uid: str):
        cooldown_sec = int(get_system_setting('STUDY_COOLDOWN_SECONDS', 15))
        flush_threshold = datetime.now() - timedelta(seconds=cooldown_sec)

        study = db.session.get(PatientStudy, study_uid)
        if not study:
            return

        zip_rule = ModalityZipRule.query.filter_by(modality_name=study.modality).first()
        batch_limit = zip_rule.max_files_per_zip if zip_rule else 50

        ready_images = ImageInstance.query.join(Series).filter(
            Series.study_uid == study_uid,
            ImageInstance.zip_status == PipelineStatus.PENDING,
            ImageInstance.compression_status == PipelineStatus.COMPLETED
        ).order_by(ImageInstance.received_at).all()

        if not ready_images:
            return

        uncompressed_count = ImageInstance.query.join(Series).filter(
            Series.study_uid == study_uid,
            ImageInstance.compression_status.in_([PipelineStatus.PENDING, PipelineStatus.PROCESSING])
        ).count()

        is_network_quiet = study.last_received < flush_threshold
        is_straggler_flush = is_network_quiet and (uncompressed_count == 0)

        # Loop and zip all full batches immediately
        while len(ready_images) >= batch_limit:
            batch_to_zip = ready_images[:batch_limit]
            for img in batch_to_zip:
                img.zip_status = PipelineStatus.PROCESSING
            db.session.commit()
            
            staged_paths_and_uids = [(img.sop_uid, img.staged_file_path) for img in batch_to_zip]
            self._pool.submit(self._execute_zip_task, study.study_uid, study.modality, staged_paths_and_uids)
            ready_images = ready_images[batch_limit:]

        # If network is quiet and no uncompressed images are left, zip the remainder
        if is_straggler_flush and len(ready_images) > 0:
            for img in ready_images:
                img.zip_status = PipelineStatus.PROCESSING
            db.session.commit()
            
            staged_paths_and_uids = [(img.sop_uid, img.staged_file_path) for img in ready_images]
            self._pool.submit(self._execute_zip_task, study.study_uid, study.modality, staged_paths_and_uids)

    def _check_stragglers(self):
        cooldown_sec = int(get_system_setting('STUDY_COOLDOWN_SECONDS', 15))
        flush_threshold = datetime.now() - timedelta(seconds=cooldown_sec)

        # Find studies with AT LEAST ONE image waiting to be zipped, where network is quiet
        active_studies = PatientStudy.query.join(Series).join(ImageInstance).filter(
            ImageInstance.zip_status == PipelineStatus.PENDING,
            PatientStudy.last_received < flush_threshold
        ).distinct().all()

        for study in active_studies:
            self._process_study(study.study_uid)

    def _execute_zip_task(self, study_uid: str, modality: str, staged_paths_and_uids: list):
        staged_paths = []
        img_uids = []
        for sop_uid, staged_path in staged_paths_and_uids:
            img_uids.append(sop_uid)
            if staged_path and os.path.exists(staged_path):
                staged_paths.append(staged_path)
        
        # 2. Perform zip operation
        os.makedirs(Config.OUTGOING_FOLDER, exist_ok=True)
        batch_count = len(img_uids)
        timestamp = int(time.time())
        unique_id = uuid.uuid4().hex[:6]
        zip_filename = f"{modality}_{study_uid}_contains_{batch_count}_images_{timestamp}_{unique_id}.zip"
        zip_filepath = os.path.join(Config.OUTGOING_FOLDER, zip_filename)
        
        zip_tmp_filepath = f"{zip_filepath}.tmp"
        success = False
        try:
            compression_mode = zipfile.ZIP_STORED if getattr(Config, 'ZIP_MODE', 'STORED') == 'STORED' else zipfile.ZIP_DEFLATED
            with zipfile.ZipFile(zip_tmp_filepath, 'w', compression_mode) as zf:
                for file_path in staged_paths:
                    zf.write(file_path, os.path.basename(file_path))
            os.rename(zip_tmp_filepath, zip_filepath)
            success = True
            log.info(f"Packaged chunk of {batch_count} images for {study_uid}")

            # Clean up staged files
            for file_path in staged_paths:
                try:
                    if os.path.exists(file_path):
                        os.remove(file_path)
                except OSError as e:
                    log.warning(f"Could not delete staged file {file_path}: {e}")
                    
            if staged_paths:
                try:
                    staged_dir = os.path.dirname(staged_paths[0])
                    if os.path.exists(staged_dir) and not os.listdir(staged_dir):
                        os.rmdir(staged_dir)
                except Exception as e:
                    pass

            # Enqueue to upload queue
            upload_payload = {
                'zip_path': zip_filepath,
                'study_uid': study_uid,
                'image_count': batch_count
            }
            # Upload queue in CQRS architecture
            upload_queue.put(upload_payload)
            
        except Exception as e:
            log.error(f"Failed to create ZIP chunk for {study_uid}: {e}", exc_info=True)

        # 3. Update final statuses
        final_status = PipelineStatus.COMPLETED if success else PipelineStatus.FAILED
        for uid in img_uids:
            db_write_queue.put(DBWriteEvent(
                action="update_status",
                payload={'entity_type': 'image', 'uid': uid, 'status_field': 'zip_status', 'status': final_status}
            ))
