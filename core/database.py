import os
import queue
import threading
import enum
from datetime import datetime, timedelta
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import event
from sqlalchemy.engine import Engine
from logger import get_enterprise_logger

log = get_enterprise_logger("DatabaseConfig")

# ==========================================
# 1. INITIALIZATION & LOCKS
# ==========================================
import contextlib
db = SQLAlchemy()
db_lock = contextlib.nullcontext()
file_lock = contextlib.nullcontext()
db_queue = queue.Queue()



@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    if dbapi_connection.__class__.__module__.startswith('sqlite3'):
        cursor = dbapi_connection.cursor()
        
        # 1. Enforce Data Integrity (Crucial for Foreign Keys)
        cursor.execute("PRAGMA foreign_keys=ON")
        
        # 2. The Speed Boost Pragmas (Safe for Legacy Hardware)
        cursor.execute("PRAGMA journal_mode=WAL")       # Concurrent reads/writes
        cursor.execute("PRAGMA synchronous=NORMAL")     # Prevent disk thrashing
        cursor.execute("PRAGMA cache_size=-64000")      # ~64MB RAM cache
        
        # 3. CONCURRENCY FIX: Wait up to 5 seconds if database is locked by another thread
        cursor.execute("PRAGMA busy_timeout=5000")
        
        cursor.close()

from core.state_machine import PipelineStatus


# ==========================================
# 2. CORE SYSTEM CONFIGURATION MODELS
# ==========================================
class AllowedScanner(db.Model):
    __tablename__ = 'allowed_scanners'
    id = db.Column(db.Integer, primary_key=True)
    ae_title = db.Column(db.String, nullable=False)
    ip_address = db.Column(db.String, nullable=False) 
    port = db.Column(db.Integer, nullable=False) 
    description = db.Column(db.String)

class AllowedModality(db.Model):
    __tablename__ = 'allowed_modalities'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(10), unique=True, nullable=False)

class ModalityZipRule(db.Model):
    __tablename__ = 'modality_zip_rules'
    id = db.Column(db.Integer, primary_key=True)
    modality_name = db.Column(
        db.String(10), 
        db.ForeignKey('allowed_modalities.name', ondelete='CASCADE'), 
        unique=True, 
        nullable=False, 
        index=True
    )
    max_files_per_zip = db.Column(db.Integer, nullable=False, default=100)
    last_updated = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now)
    
    allowed_modality = db.relationship(
        'AllowedModality', 
        backref=db.backref('zip_rule', uselist=False, cascade='all, delete-orphan')
    )


class ModalityCompressionRule(db.Model):
    __tablename__ = 'modality_compression_rules'
    id = db.Column(db.Integer, primary_key=True)
    
    modality_name = db.Column(
        db.String(10), 
        db.ForeignKey('allowed_modalities.name', ondelete='CASCADE'), 
        unique=True, 
        nullable=False, 
        index=True
    )
    
    # Can be 'LOSSY', 'LOSSLESS', or 'COPY'
    compression_type = db.Column(db.String(20), nullable=False, default='LOSSLESS') 
    
    # 0 to 100 (Used for LOSSY quality limits)
    compression_level = db.Column(db.Integer, nullable=False, default=100) 
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now)

    # Isolated relationship back to the Allowed Modalities list
    allowed_modality = db.relationship(
        'AllowedModality', 
        backref=db.backref('compression_rule', uselist=False, cascade='all, delete-orphan')
    )

    def to_dict(self):
        return {
            "id": self.id,
            "modality_name": self.modality_name,
            "compression_type": self.compression_type,
            "compression_level": self.compression_level
        }

# ==========================================
# 3. MULTI-TENANT CENTER MANAGEMENT
# ==========================================
class Center(db.Model):
    __tablename__ = 'centers'
    id = db.Column(db.Integer, primary_key=True)
    center_code = db.Column(db.String(50), unique=True, nullable=False, index=True) 
    name = db.Column(db.String(255), nullable=False) 

    # Reverse relationships to easily access mapped data
    mappings = db.relationship('InstitutionMapping', backref='center', lazy=True, cascade='all, delete-orphan')
    modalities = db.relationship('CenterModality', backref='center', lazy=True, cascade='all, delete-orphan')
    studies = db.relationship('PatientStudy', backref='center', lazy=True)

class InstitutionMapping(db.Model):
    """Maps the messy DICOM InstitutionName (0008,0080) to your clean Center ID"""
    __tablename__ = 'institution_mappings'
    id = db.Column(db.Integer, primary_key=True)
    dicom_institution_name = db.Column(db.String(255), unique=True, nullable=False, index=True)
    center_id = db.Column(db.Integer, db.ForeignKey('centers.id', ondelete='CASCADE'), nullable=False)

class CenterModality(db.Model):
    """Security Matrix: Which modalities are allowed for which Center?"""
    __tablename__ = 'center_modalities'
    id = db.Column(db.Integer, primary_key=True)
    center_id = db.Column(db.Integer, db.ForeignKey('centers.id', ondelete='CASCADE'), nullable=False)
    modality_name = db.Column(db.String(10), db.ForeignKey('allowed_modalities.name', ondelete='CASCADE'), nullable=False)

    __table_args__ = (
        db.UniqueConstraint('center_id', 'modality_name', name='_center_modality_uc'),
    )


# ==========================================
# 4. DICOM DATA HIERARCHY
# ==========================================
class PatientStudy(db.Model):
    __tablename__ = 'patients'
    
    # DICOM Identifiers
    study_uid = db.Column(db.String, primary_key=True)
    patient_id = db.Column(db.String)
    patient_name = db.Column(db.String)
    patient_age = db.Column(db.String)   
    patient_sex = db.Column(db.String)   
    modality = db.Column(db.String)
    study_date = db.Column(db.String)
    study_description = db.Column(db.String)
    outgoing_description = db.Column(db.String(255), nullable=True)
    institution_name = db.Column(db.String(255))
    patient_history = db.Column(db.Text, nullable=True)
    ref_physician_name = db.Column(db.String(255))
    physician_id = db.Column(db.Integer, db.ForeignKey('referring_physicians.id', ondelete='SET NULL'), nullable=True, index=True)
    physician = db.relationship('ReferringPhysician', backref=db.backref('studies', lazy=True))

    # ---------------------------
    accession_number = db.Column(db.String(255), nullable=True)
    patient_birth_date = db.Column(db.String(50), nullable=True)
    manufacturer = db.Column(db.String(255), nullable=True)
    manufacturer_model_name = db.Column(db.String(255), nullable=True)
    # --- MULTI-TENANT LINK ---
    center_id = db.Column(db.Integer, db.ForeignKey('centers.id', ondelete='SET NULL'), nullable=True, index=True)
    
    # Pipeline Metadata
    image_count = db.Column(db.Integer, default=0)
    uploaded_image_count = db.Column(db.Integer, default=0)
    last_received = db.Column(db.DateTime, default=datetime.now)
    compression_status = db.Column(db.Enum(PipelineStatus), default=PipelineStatus.PENDING, nullable=False, index=True)
    zip_status = db.Column(db.Enum(PipelineStatus), default=PipelineStatus.PENDING, nullable=False, index=True)
    upload_status = db.Column(db.Enum(PipelineStatus), default=PipelineStatus.PENDING, nullable=False, index=True)
    retry_count = db.Column(db.Integer, default=0, nullable=False)
    last_upload_attempt = db.Column(db.DateTime, nullable=True)
    remote_upload_id = db.Column(db.String(255), nullable=True)
    is_paused = db.Column(db.Boolean, default=False, nullable=False)
    checksum = db.Column(db.String(64), nullable=True)
    api_synced_image_count = db.Column(db.Integer, default=0)

    # Relationships
    series = db.relationship('Series', backref='study', lazy=True, cascade='all, delete-orphan', passive_deletes=True)

    __table_args__ = (
        db.Index('ix_patient_last_received', 'last_received'),
        db.Index('ix_patient_study_date', 'study_date'),
        db.Index('ix_patient_modality_received', 'modality', 'last_received'),
        db.Index('ix_worker_upload_queue', 'upload_status', 'retry_count'),
    )

    @property
    def images(self):
        """Helper: Gathers all images from all nested series."""
        all_images = []
        for s in self.series:
            all_images.extend(s.images)
        return all_images
    
    @property
    def ui_status(self) -> str:
        """Translates continuous streaming state to a clean frontend string."""
        from datetime import datetime, timedelta
        
        # 🚨 NEW: 0. FATAL MAPPING ERRORS (Highest Priority)
        if self.remote_upload_id == "UNMAPPED_CENTER":
            return "Unmapped ⚠️"

        # 1. Manual Overrides & Errors
        if self.is_paused:
            return "Paused ⏸️"
            
        if self.upload_status == PipelineStatus.FAILED:
            return "Network Paused"

        # 2. Completion State
        if self.image_count > 0 and self.uploaded_image_count >= self.image_count:
            return "Uploaded"

        # 3. Active Streaming State
        if self.uploaded_image_count > 0 and self.uploaded_image_count < self.image_count:
            return "Streaming"

        # 4. Network Receiving State 
        if self.last_received:
            cutoff_time = datetime.now() - timedelta(seconds=15)
            if self.last_received > cutoff_time:
                return "Receiving"

        # 5. Intermediate/Processing State
        if self.image_count > 0 and self.uploaded_image_count == 0:
            return "Processing"

        return "Pending"

class Series(db.Model):
    __tablename__ = 'series'
    series_uid = db.Column(db.String, primary_key=True)
    study_uid = db.Column(db.String, db.ForeignKey('patients.study_uid', ondelete='CASCADE'), index=True, nullable=False)
    series_number = db.Column(db.Integer)
    series_description = db.Column(db.String)
    images = db.relationship('ImageInstance', backref='series', lazy=True, cascade='all, delete-orphan', passive_deletes=True)

class ImageInstance(db.Model):
    __tablename__ = 'images'
    sop_uid = db.Column(db.String, primary_key=True)
    series_uid = db.Column(db.String, db.ForeignKey('series.series_uid', ondelete='CASCADE'), index=True, nullable=False)
    instance_number = db.Column(db.Integer)
    file_path = db.Column(db.String, nullable=False)
    received_at = db.Column(db.DateTime, default=datetime.now)
    window_width = db.Column(db.String(100), nullable=True)
    window_center = db.Column(db.String(100), nullable=True)
    slice_thickness = db.Column(db.String(100), nullable=True)
    acquisition_time = db.Column(db.String(100), nullable=True)
    slice_location = db.Column(db.String(100), nullable=True)
    columns = db.Column(db.String(50), nullable=True)
    rows = db.Column(db.String(50), nullable=True)
    photometric_interpretation = db.Column(db.String(50), nullable=True)
    bits_allocated = db.Column(db.String(50), nullable=True)
    bits_stored = db.Column(db.String(50), nullable=True)
    pixel_representation = db.Column(db.String(50), nullable=True)
    samples_per_pixel = db.Column(db.String(50), nullable=True)
    pixel_spacing = db.Column(db.String(100), nullable=True)
    image_orientation_patient = db.Column(db.String(255), nullable=True)
    image_position_patient = db.Column(db.String(255), nullable=True)
    frame_of_reference_uid = db.Column(db.String(255), nullable=True)
    image_type = db.Column(db.String(255), nullable=True)
    sop_class_uid = db.Column(db.String(255), nullable=True)
    high_bit = db.Column(db.String(50), nullable=True)
    compression_status = db.Column(db.Enum(PipelineStatus), default=PipelineStatus.PENDING, nullable=False, index=True)
    zip_status = db.Column(db.Enum(PipelineStatus), default=PipelineStatus.PENDING, nullable=False, index=True)
    is_api_synced = db.Column(db.Boolean, default=False, nullable=False)
    staged_file_path = db.Column(db.String, nullable=True)
# ==========================================
# 5. DICTIONARY & MAPPING MODELS
# ==========================================
class DescriptionRule(db.Model):
    __tablename__ = 'description_rules'
    rule_id = db.Column(db.String(50), primary_key=True) 
    original_description = db.Column(db.String(255), nullable=False)
    modality = db.Column(db.String(50), nullable=False)

class DescriptionMapping(db.Model):
    __tablename__ = 'description_mappings'
    id = db.Column(db.Integer, primary_key=True)
    machine_description = db.Column(db.String(255), nullable=False, index=True)
    rule_id = db.Column(db.String(50), db.ForeignKey('description_rules.rule_id'), nullable=False)
    rule = db.relationship('DescriptionRule', backref='mappings')


# ==========================================
# 6. LOGGING & SYSTEM STATE
# ==========================================
class AuditLog(db.Model):
    __tablename__ = 'audit_logs'
    id = db.Column(db.Integer, primary_key=True)
    timestamp = db.Column(db.DateTime, default=datetime.now, index=True)
    action_type = db.Column(db.String, index=True, nullable=False)
    study_uid = db.Column(db.String, index=True)
    sop_uid = db.Column(db.String)
    source = db.Column(db.String) 
    __table_args__ = (
        db.Index('ix_audit_action_time', 'action_type', 'timestamp'),
    )

class SystemSetting(db.Model):
    __tablename__ = 'system_settings'
    key = db.Column(db.String(50), primary_key=True)
    value = db.Column(db.String(255), nullable=False)


# ==========================================
# NEW: REFERRING PHYSICIAN MANAGEMENT
# ==========================================
class ReferringPhysician(db.Model):
    __tablename__ = 'referring_physicians'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), unique=True, nullable=False, index=True)
   
# ==========================================
# 7. QUEUE UTILITIES
# ==========================================
def queue_study_update(payload: dict):
    """Drops the DICOM data dictionary into the Inbox Basket via EventBus."""
    from core.event_bus import db_write_queue, DBWriteEvent
    try:
        event = DBWriteEvent(action="insert_dicom", payload=payload)
        db_write_queue.put(event, timeout=2.0)
        return True
    except queue.Full:
        log.warning("Queue is completely full, dropping database update")
        temp_path = payload.get('temp_incoming')
        if temp_path and os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except Exception:
                log.error("Could not remove orphaned temp file", exc_info=True)
        return False
    


def get_system_setting(key, default_value=""):
    """Safely fetches a setting from the database, falling back to a default if missing."""
    try:
        setting = SystemSetting.query.filter_by(key=key).first()
        if setting and setting.value.strip():
            return setting.value.strip()
    except Exception:
        pass
    return default_value