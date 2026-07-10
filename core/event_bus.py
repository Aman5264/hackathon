import queue
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

# ==========================================
# CENTRALIZED PIPELINE QUEUES
# ==========================================
# These queues decouple the components.
db_write_queue = queue.Queue(maxsize=10000)
compression_queue = queue.Queue(maxsize=1000)
zip_queue = queue.Queue(maxsize=1000)
upload_queue = queue.Queue(maxsize=1000)


# ==========================================
# EVENT DEFINITIONS
# ==========================================
@dataclass
class DicomReceivedEvent:
    """Event fired when a new DICOM file is received and validated."""
    file_path: str
    metadata: Dict[str, Any]
    temp_incoming_path: Optional[str] = None

@dataclass
class DBWriteEvent:
    """Event to instruct the single DatabaseWriter thread to perform an action."""
    action: str  # e.g., 'insert_study', 'update_image_status', 'log_audit'
    payload: Dict[str, Any]

@dataclass
class CompressionTaskEvent:
    """Instructs the compression worker to process an image."""
    sop_uid: str
    study_uid: str
    file_path: str

@dataclass
class ZipTaskEvent:
    """Instructs the zip worker to bundle a study."""
    study_uid: str
    image_sops: list[str] = field(default_factory=list)

@dataclass
class UploadTaskEvent:
    """Instructs the upload worker to upload a study."""
    study_uid: str
