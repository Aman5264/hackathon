import threading
import queue
import time
import os
from datetime import datetime
from sqlalchemy.exc import IntegrityError
from flask import Flask

from core.event_bus import db_write_queue, DBWriteEvent
from core.database import db, PatientStudy, Series, ImageInstance, AuditLog
from core.state_machine import PipelineStatus
from logger import get_enterprise_logger

log = get_enterprise_logger("DBWriter")

class DatabaseWriterService(threading.Thread):
    def __init__(self, app: Flask):
        super().__init__(name="DBWriterService", daemon=True)
        self.app = app
        self._shutdown_flag = threading.Event()
        self.processed_count = 0

    def shutdown(self):
        self._shutdown_flag.set()

    def run(self):
        log.info("DatabaseWriterService started. Listening on db_write_queue.")
        while not self._shutdown_flag.is_set():
            try:
                with self.app.app_context():
                    self._safe_run()
            except Exception as e:
                log.critical(f"FATAL THREAD CRASH in DBWriter. Restarting in 5s. Error: {e}", exc_info=True)
                time.sleep(5)

    def _safe_run(self):
        # Run a quick startup cleanup for any mismatched counts
        try:
            corrupt_studies = PatientStudy.query.filter(PatientStudy.uploaded_image_count > PatientStudy.image_count).all()
            if corrupt_studies:
                for c in corrupt_studies:
                    c.uploaded_image_count = c.image_count
                self._safe_commit()
                log.info(f"Fixed {len(corrupt_studies)} studies with uploaded_image_count mismatches.")
        except Exception as e:
            db.session.rollback()
            log.warning(f"Could not run mismatch cleanup: {e}")

        while not self._shutdown_flag.is_set():
            try:
                event: DBWriteEvent = db_write_queue.get(timeout=1.0)
            except queue.Empty:
                continue

            if event.action == "SHUTDOWN":
                log.info("DatabaseWriterService clocking out safely")
                db_write_queue.task_done()
                break

            try:
                self._handle_event(event)
            except Exception as e:
                log.error(f"Error handling DBWriteEvent {event.action}: {e}", exc_info=True)
                db.session.rollback()
            finally:
                db_write_queue.task_done()

    def _safe_commit(self):
        """Robust exponential backoff for SQLite lockouts."""
        for attempt in range(4):
            try:
                db.session.commit()
                return
            except Exception as e:
                db.session.rollback()
                if attempt == 3:
                    log.error(f"Database completely locked after 4 attempts. Commits dropped: {e}")
                    raise e
                time.sleep(0.1 * (2 ** attempt))

    def _handle_event(self, event: DBWriteEvent):
        if event.action == "insert_dicom":
            self._handle_insert_dicom(event.payload)
        elif event.action == "update_status":
            self._handle_update_status(event.payload)
        elif event.action == "log_audit":
            self._handle_log_audit(event.payload)
        elif event.action == "update_image_staged_path":
            self._handle_update_image_staged_path(event.payload)
        elif event.action == "update_study_remote_id":
            self._handle_update_study_remote_id(event.payload)
        elif event.action == "update_upload_success":
            self._handle_update_upload_success(event.payload)
        else:
            log.warning(f"Unknown DBWriteEvent action: {event.action}")

    def _handle_insert_dicom(self, payload: dict):
        s_uid = payload['study_uid']
        series_uid = payload['series_uid']
        sop_uid = payload['sop_uid']
        final_path = payload['final_incoming']
        
        # In this architecture, raw_desc to official_desc mapping is handled before
        # or inside DBWriter. For now, we take from payload.
        raw_desc = payload.get('study_description', '')
        official_desc = payload.get('outgoing_description', '')
        
        try:
            # 1. STUDY
            study = PatientStudy.query.filter_by(study_uid=s_uid).first()
            if not study:
                study = PatientStudy(
                    study_uid=s_uid,
                    patient_id=payload.get('patient_id'),
                    patient_name=payload.get('patient_name'),
                    patient_age=payload.get('patient_age'),
                    patient_sex=payload.get('patient_sex'),
                    patient_birth_date=payload.get('patient_birth_date'),
                    modality=payload.get('modality'),
                    study_date=payload.get('study_date'),
                    study_description=raw_desc,
                    outgoing_description=official_desc,
                    institution_name=payload.get('institution_name'),
                    ref_physician_name=payload.get('ref_physician_name'),
                    accession_number=payload.get('accession_number'),
                    manufacturer=payload.get('manufacturer'),
                    manufacturer_model_name=payload.get('manufacturer_model_name'),
                    image_count=0
                )
                db.session.add(study)
                db.session.add(AuditLog(action_type='STUDY_CREATED', study_uid=s_uid, sop_uid=sop_uid, source='NETWORK'))
            else:
                study.last_received = datetime.now()

            # 2. SERIES
            series = Series.query.filter_by(series_uid=series_uid).first()
            if not series:
                series = Series(
                    series_uid=series_uid,
                    study_uid=s_uid,
                    series_number=payload.get('series_number'),
                    series_description=payload.get('series_description')
                )
                db.session.add(series)

            # 3. IMAGE
            image = ImageInstance.query.filter_by(sop_uid=sop_uid).first()
            if not image:
                new_image = ImageInstance(
                    sop_uid=sop_uid,
                    series_uid=series_uid,
                    instance_number=payload.get('instance_number'),
                    file_path=final_path,
                    window_width=payload.get('window_width'),
                    window_center=payload.get('window_center'),
                    slice_thickness=payload.get('slice_thickness'),
                    acquisition_time=payload.get('acquisition_time'),
                    slice_location=payload.get('slice_location'),
                    columns=payload.get('columns'),
                    rows=payload.get('rows'),
                    photometric_interpretation=payload.get('photometric_interpretation'),
                    bits_allocated=payload.get('bits_allocated'),
                    bits_stored=payload.get('bits_stored'),
                    pixel_representation=payload.get('pixel_representation'),
                    samples_per_pixel=payload.get('samples_per_pixel'),
                    pixel_spacing=payload.get('pixel_spacing'),
                    image_orientation_patient=payload.get('image_orientation_patient'),
                    image_position_patient=payload.get('image_position_patient'),
                    frame_of_reference_uid=payload.get('frame_of_reference_uid'),
                    image_type=payload.get('image_type'),
                    sop_class_uid=payload.get('sop_class_uid'),
                    high_bit=payload.get('high_bit')
                )
                db.session.add(new_image)
                study.image_count += 1
                db.session.add(AuditLog(action_type='IMAGE_RECEIVED', study_uid=s_uid, sop_uid=sop_uid, source='NETWORK'))

            self._safe_commit()
            self.processed_count += 1
            
            # TRIGGER COMPRESSION INSTANTLY
            from core.event_bus import compression_queue, CompressionTaskEvent
            try:
                compression_queue.put(CompressionTaskEvent(
                    sop_uid=sop_uid,
                    study_uid=s_uid,
                    file_path=final_path
                ), block=False)
                log.info(f"Successfully pushed compression task to queue for SOP {sop_uid}")
            except Exception as e:
                log.error(f"Failed to queue compression task for SOP {sop_uid}: {e}")
            
        except IntegrityError:
            db.session.rollback()
            log.info(f"Deduplication: SOP {sop_uid} already exists. Attempting to heal study status.")
            
            # Heal the study if it was stuck
            study = PatientStudy.query.filter_by(study_uid=s_uid).first()
            if study and study.upload_status == PipelineStatus.FAILED:
                study.upload_status = PipelineStatus.PENDING
                study.retry_count = 0
                self._safe_commit()
                log.info(f"Healed Study {s_uid}: Reset upload status to PENDING.")
            
            # CRITICAL FIX: Delete the duplicate file from Incoming_Data to prevent disk leaks!
            final_path = payload.get('final_incoming')
            if final_path and os.path.exists(final_path):
                try:
                    os.remove(final_path)
                except Exception as e:
                    log.warning(f"Failed to delete duplicate file {final_path}: {e}")
        except Exception as e:
            db.session.rollback()
            log.error(f"Transaction failed for {sop_uid}: {e}", exc_info=True)
            raise

    def _handle_update_status(self, payload: dict):
        entity_type = payload.get('entity_type') # 'study' or 'image'
        uid = payload.get('uid')
        status_field = payload.get('status_field') # 'compression_status', 'zip_status', 'upload_status'
        new_status = payload.get('status') # PipelineStatus enum instance
        
        try:
            if entity_type == 'study':
                obj = PatientStudy.query.filter_by(study_uid=uid).first()
            elif entity_type == 'image':
                obj = ImageInstance.query.filter_by(sop_uid=uid).first()
            else:
                return

            if obj and hasattr(obj, status_field):
                setattr(obj, status_field, new_status)
                self._safe_commit()
        except Exception as e:
            db.session.rollback()
            log.error(f"Failed to update status for {entity_type} {uid}: {e}")

    def _handle_log_audit(self, payload: dict):
        try:
            log_entry = AuditLog(
                action_type=payload.get('action_type'),
                study_uid=payload.get('study_uid'),
                sop_uid=payload.get('sop_uid'),
                source=payload.get('source')
            )
            db.session.add(log_entry)
            self._safe_commit()
        except Exception as e:
            db.session.rollback()
            log.error(f"Failed to log audit: {e}")

    def _handle_update_image_staged_path(self, payload: dict):
        try:
            sop_uid = payload.get('sop_uid')
            staged_file_path = payload.get('staged_file_path')
            img = db.session.get(ImageInstance, sop_uid)
            if img:
                img.staged_file_path = staged_file_path
                self._safe_commit()
        except Exception as e:
            db.session.rollback()
            log.error(f"Failed to update staged path for {payload.get('sop_uid')}: {e}")

    def _handle_update_study_remote_id(self, payload: dict):
        try:
            study_uid = payload.get('study_uid')
            study = db.session.get(PatientStudy, study_uid)
            if study:
                study.remote_upload_id = payload.get('remote_upload_id')
                if 'upload_status' in payload:
                    study.upload_status = payload['upload_status']
                self._safe_commit()
        except Exception as e:
            db.session.rollback()
            log.error(f"Failed to update remote id for {payload.get('study_uid')}: {e}")

    def _handle_update_upload_success(self, payload: dict):
        try:
            study_uid = payload.get('study_uid')
            image_count = payload.get('image_count')
            study = db.session.get(PatientStudy, study_uid)
            if study:
                new_total = study.uploaded_image_count + image_count
                study.uploaded_image_count = min(study.image_count, new_total)
                study.upload_status = PipelineStatus.COMPLETED
                study.retry_count = 0
                self._safe_commit()
                
                # Instantly notify the remote server of the new uploaded count
                import threading
                def _bg_sync(app, s_uid, u_count, t_count):
                    with app.app_context():
                        from workers.api_sync_worker import sync_upload_count
                        sync_upload_count(s_uid, u_count, t_count)
                
                threading.Thread(
                    target=_bg_sync,
                    args=(self.app, study.study_uid, study.uploaded_image_count, study.image_count),
                    daemon=True
                ).start()
                
        except Exception as e:
            db.session.rollback()
            log.error(f"Failed to update upload success for {payload.get('study_uid')}: {e}")
