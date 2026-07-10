import os
import sys
import shutil
import subprocess
import threading
import queue
import pydicom

from flask import Flask
from core.event_bus import compression_queue, db_write_queue, DBWriteEvent, CompressionTaskEvent, ZipTaskEvent
from core.state_machine import PipelineStatus
from logger import get_enterprise_logger
from config import Config

log = get_enterprise_logger("CompressionWorker")

def get_binary_path(binary_name: str) -> str:
    if getattr(sys, 'frozen', False):
        base_dir = os.path.dirname(sys.executable)
    else:
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.abspath(os.path.join(base_dir, 'tools', binary_name))

class CompressionWorkerService(threading.Thread):
    def __init__(self, app: Flask):
        super().__init__(name="CompressionWorker", daemon=True)
        self.app = app
        self._shutdown_flag = threading.Event()

    def shutdown(self):
        self._shutdown_flag.set()

    def run(self):
        log.info("CompressionWorkerService started. Listening on compression_queue.")
        while not self._shutdown_flag.is_set():
            try:
                with self.app.app_context():
                    self._safe_run()
            except Exception as e:
                log.critical(f"FATAL THREAD CRASH in CompressionWorker. Restarting in 5s. Error: {e}", exc_info=True)
                import time
                time.sleep(5)

    def _safe_run(self):
        while not self._shutdown_flag.is_set():
            try:
                task: CompressionTaskEvent = compression_queue.get(timeout=1.0)
            except queue.Empty:
                continue

            if getattr(task, 'action', '') == "SHUTDOWN":
                compression_queue.task_done()
                break

            try:
                self._process_task(task)
            except Exception as e:
                log.error(f"Error processing compression task {task.sop_uid}: {e}", exc_info=True)
            finally:
                compression_queue.task_done()

    def _process_task(self, task: CompressionTaskEvent):
        log.info(f'Started compression task for SOP: {task.sop_uid}')

        # Notify DB that we are processing
        db_write_queue.put(DBWriteEvent(
            action="update_status",
            payload={'entity_type': 'image', 'uid': task.sop_uid, 'status_field': 'compression_status', 'status': PipelineStatus.PROCESSING}
        ))

        # Output path
        output_dir = os.path.join(Config.PROCESSING_FOLDER, task.study_uid)
        os.makedirs(output_dir, exist_ok=True)
        target_path = os.path.join(output_dir, os.path.basename(task.file_path))

        # Compress
        from core.database import ModalityCompressionRule, PatientStudy, Series, ImageInstance, db
        # Read modality from DB (read is safe and fast in WAL mode)
        img = db.session.get(ImageInstance, task.sop_uid)
        modality = "UNKNOWN"
        if img and img.series and img.series.study:
            modality = img.series.study.modality

        success = self._compress_single_dicom(task.file_path, target_path, modality)

        # Notify DB of result
        if success:
            # Delete the original raw file from Incoming_Data to save space
            try:
                if os.path.exists(task.file_path):
                    os.remove(task.file_path)
            except Exception as e:
                log.warning(f"Could not delete original file {task.file_path}: {e}")

            # Update image status to completed
            db_write_queue.put(DBWriteEvent(
                action="update_status",
                payload={'entity_type': 'image', 'uid': task.sop_uid, 'status_field': 'compression_status', 'status': PipelineStatus.COMPLETED}
            ))
            # Also update the staged file path 
            # (In a real CQRS we might have a specific event for this, but for now we can do a generic update or let DBWriter handle it via an action)
            db_write_queue.put(DBWriteEvent(
                action="update_image_staged_path",
                payload={'sop_uid': task.sop_uid, 'staged_file_path': target_path}
            ))
            
            # Add a zip task event to trigger zipping. The zip worker will debounce or bundle.
            from core.event_bus import zip_queue
            zip_queue.put(ZipTaskEvent(study_uid=task.study_uid))
        else:
            db_write_queue.put(DBWriteEvent(
                action="update_status",
                payload={'entity_type': 'image', 'uid': task.sop_uid, 'status_field': 'compression_status', 'status': PipelineStatus.FAILED}
            ))

    def _get_compression_settings(self, modality: str):
        clean_modality = str(modality).strip().upper()
        try:
            from core.database import ModalityCompressionRule
            rule = ModalityCompressionRule.query.filter_by(modality_name=clean_modality).first()
            if rule:
                return rule.compression_type, rule.compression_level
        except Exception as e:
            log.error(f"Failed to fetch compression rule for {clean_modality}: {e}")
        return 'LOSSLESS', 100

    def _compress_single_dicom(self, input_path: str, output_path: str, modality: str) -> bool:
        if not os.path.exists(input_path):
            return False

        comp_type, quality = self._get_compression_settings(modality)
        filename = os.path.basename(input_path)

        if comp_type == 'COPY':
            try:
                shutil.copy2(input_path, output_path)
                return True
            except Exception as e:
                log.error(f"Copy failed for {filename}: {e}")
                return False

        try:
            ds = pydicom.dcmread(input_path, stop_before_pixels=True)
            if ds.file_meta.TransferSyntaxUID.is_compressed:
                shutil.copy2(input_path, output_path)
                return True
        except Exception:
            pass

        dcmtk_exe = get_binary_path("dcmcjpeg.exe")
        if not os.path.exists(dcmtk_exe):
            log.critical(f"Missing required binary at: {dcmtk_exe}. Falling back to COPY.")
            shutil.copy2(input_path, output_path)
            return True

        temp_out = output_path + ".tmp"
        cmd = [dcmtk_exe]
        
        if comp_type == 'LOSSLESS':
            cmd.append("+e1") 
        else:
            cmd.extend(["+ee", "+q", str(max(10, min(100, quality)))]) 
            
        cmd.extend([input_path, temp_out])

        try:
            result = subprocess.run(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=20, text=True,
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0
            )

            if result.returncode == 0 and os.path.exists(temp_out):
                os.replace(temp_out, output_path)
                log.info(f"DCMTK Success ({comp_type}): {filename}")
                return True
            else:
                log.error(f"DCMTK Failed for {filename}: {result.stderr}")
                if os.path.exists(temp_out):
                    os.remove(temp_out)
                shutil.copy2(input_path, output_path)
                return True

        except subprocess.TimeoutExpired:
            log.error(f"DCMTK timeout on {filename}")
            if os.name == 'nt':
                subprocess.run(['taskkill', '/F', '/IM', 'dcmcjpeg.exe'], capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
            if os.path.exists(temp_out):
                os.remove(temp_out)
            shutil.copy2(input_path, output_path)
            return True
        except Exception as e:
            log.error(f"Compression system error: {e}")
            shutil.copy2(input_path, output_path)
            return True
