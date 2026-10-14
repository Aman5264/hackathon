import sys
import os
import time
import json
import zipfile
import pydicom
from pydicom.data import get_testdata_file
from io import BytesIO

# Ensure paths
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from app import app, start_radnova_pipeline
from core.database import db, PatientStudy, ensure_schema_compatibility
from config import Config

print("=======================================================")
print("  RADNOVA HACKATHON MVP: END-TO-END VERIFICATION TEST ")
print("=======================================================")

# Initialize config and directories
Config.init_directories()

with app.app_context():
    db.create_all()
    ensure_schema_compatibility(app)
    # Clean up any prior test run record so we start fresh
    _test_uid = '1.3.6.1.4.1.5962.1.2.1.20040119072730.12322'
    _existing = db.session.get(PatientStudy, _test_uid)
    if _existing:
        db.session.delete(_existing)
        db.session.commit()
        print(f"[Setup] Cleared prior test record for study {_test_uid}")

# Start Radnova background workers
start_radnova_pipeline(app)
time.sleep(2)

client = app.test_client()

# -------------------------------------------------------------
# 1. TEST INVALID FILE REJECTION
# -------------------------------------------------------------
print("\n[Step 1] Uploading invalid non-DICOM file...")
invalid_data = {
    'dicom_files': (BytesIO(b'NOT_A_VALID_DICOM_CONTENT_FOR_TESTING'), 'bad_file.txt')
}
res = client.post('/upload', data=invalid_data, content_type='multipart/form-data')
print("Status code:", res.status_code)
resp_json = res.get_json()
print("Response:", resp_json)
assert resp_json.get('failed', 0) > 0, "Expected invalid file to be rejected"
print("PASS: Invalid non-DICOM file rejected safely without crashing.")

# -------------------------------------------------------------
# 2. TEST VALID DE-IDENTIFIED DICOM UPLOAD
# -------------------------------------------------------------
print("\n[Step 2] Uploading valid de-identified DICOM study (CT_small.dcm)...")
dcm_path = get_testdata_file('CT_small.dcm')
with open(dcm_path, 'rb') as f:
    valid_bytes = f.read()

valid_data = {
    'dicom_files': (BytesIO(valid_bytes), 'CT_small.dcm')
}
res = client.post('/upload', data=valid_data, content_type='multipart/form-data')
print("Status code:", res.status_code)
resp_json = res.get_json()
print("Response:", resp_json)
assert resp_json.get('processed', 0) > 0, "Expected valid DICOM file to be processed"
study_uid = resp_json['affected_studies'][0]
print(f"Ingested Study UID: {study_uid}")
print("PASS: Valid DICOM file accepted and enqueued.")

# -------------------------------------------------------------
# 3. TEST ASYNCHRONOUS PIPELINE STAGES
# -------------------------------------------------------------
print("\n[Step 3] Waiting for pipeline stages (Compression -> ZIP -> Gemma -> Upload)...")
max_wait = 12
start_t = time.time()
study_record = None

while time.time() - start_t < max_wait:
    with app.app_context():
        s = db.session.get(PatientStudy, study_uid)
        if s and s.zip_status.value == 'COMPLETED' and s.report_status in ('DRAFT', 'COMPLETED', 'DEMO_MODE'):
            study_record = s
            break
    time.sleep(1)

with app.app_context():
    s = db.session.get(PatientStudy, study_uid)
    assert s is not None, "Study must exist in database"
    
    print(f" - Study UID: {s.study_uid}")
    print(f" - Patient: {s.patient_name} (ID: {s.patient_id})")
    print(f" - Modality: {s.modality}")
    print(f" - Image Count: {s.image_count}")
    print(f" - Compression: {s.compression_status.value}")
    print(f" - ZIP Status: {s.zip_status.value}")
    print(f" - Archive Path: {s.archive_path}")
    print(f" - Archive Size: {s.archive_size} bytes")
    print(f" - Archive Checksum: {s.archive_checksum}")
    print(f" - Gemma Report Status: {s.report_status}")
    print(f" - Remote Upload Status: {s.upload_status.value} (ID: {s.remote_upload_id})")

    # 4. VERIFY ZIP INTEGRITY
    print("\n[Step 4] Verifying ZIP integrity...")
    assert s.archive_path and os.path.exists(s.archive_path), "Archive file must exist on disk"
    with zipfile.ZipFile(s.archive_path, 'r') as zf:
        assert zf.testzip() is None, "ZIP file failed integrity verification test"
        namelist = zf.namelist()
        print(f" - Files inside archive: {namelist}")
    print("PASS: ZIP archive created and integrity verified!")

    # 5. VERIFY GEMMA FINDINGS REPORT
    print("\n[Step 5] Verifying Gemma Draft Findings report...")
    assert s.report_json, "Gemma report JSON must be present in database"
    report = json.loads(s.report_json)
    print(" - Model:", report.get('model'))
    print(" - Technique:", report.get('technique'))
    print(" - Findings:", report.get('findings'))
    print(" - Impression:", report.get('impression'))
    print(" - Radiologist Review Required:", report.get('requires_radiologist_review'))
    assert report.get('requires_radiologist_review') is True, "Radiologist review flag must be True"
    print("PASS: Structured draft findings report generated with mandatory review flag!")

    print("\n[Step 6] Verifying remote upload handling (waiting for async upload worker)...")

# Poll outside the context block so we can re-open a fresh session each iteration
upload_deadline = time.time() + 15
upload_ok = False
while time.time() < upload_deadline:
    with app.app_context():
        s6 = db.session.get(PatientStudy, study_uid)
        if s6 and s6.remote_upload_id == 'LOCAL_STANDALONE':
            upload_ok = True
            break
    time.sleep(1)

with app.app_context():
    s6 = db.session.get(PatientStudy, study_uid)
    print(f" - Pipeline Stage: {getattr(s6.pipeline_stage, 'value', str(s6.pipeline_stage))}")
    print(f" - Upload status: {s6.upload_status.value}")
    print(f" - Remote upload ID: {s6.remote_upload_id}")
    assert s6.upload_status.value == 'PENDING', \
        f"Upload status should be PENDING when remote server is unconfigured, got: {s6.upload_status.value}"
    assert s6.remote_upload_id == 'LOCAL_STANDALONE', \
        f"Remote ID should be 'LOCAL_STANDALONE', got: {s6.remote_upload_id!r}"
    assert getattr(s6.pipeline_stage, 'value', str(s6.pipeline_stage)) in ('COMPLETED', 'UPLOADING'), \
        f"Expected study stage COMPLETED or UPLOADING, got: {s6.pipeline_stage}"
    print("PASS: Graceful handling of unconfigured remote server (archive preserved locally).")

# -------------------------------------------------------------
# 7. TEST REST API ENDPOINTS
# -------------------------------------------------------------
print("\n[Step 7] Testing REST API endpoints...")
# /api/studies
res_studies = client.get('/api/studies')
assert res_studies.status_code == 200
studies_json = res_studies.get_json()
count = studies_json['count']
print(f" - /api/studies returned {count} study/studies.")
assert count > 0

# /api/studies/<uid>/report
res_report = client.get(f'/api/studies/{study_uid}/report')
assert res_report.status_code == 200
report_api_json = res_report.get_json()
assert report_api_json['requires_radiologist_review'] is True
print(" - /api/studies/<uid>/report returned valid draft findings JSON.")

# /api/studies/<uid>/download_zip
res_zip = client.get(f'/api/studies/{study_uid}/download_zip')
assert res_zip.status_code == 200
assert len(res_zip.data) == s.archive_size
print(f" - /api/studies/<uid>/download_zip downloaded valid {len(res_zip.data)} byte archive.")

# / (Dashboard HTML)
res_dash = client.get('/')
assert res_dash.status_code == 200
assert b'Radnova' in res_dash.data
print(" - / rendered Radnova Dashboard successfully.")

print("\n=======================================================")
print("  ALL 7 END-TO-END RADNOVA PIPELINE CHECKS PASSED!     ")
print("=======================================================")
