import threading
import time
import os
import requests
from datetime import datetime
from core.database import db, db_lock, PatientStudy, SystemSetting, DescriptionMapping, DescriptionRule, InstitutionMapping
from logger import get_enterprise_logger
from config import Config
from mapper import get_mapped_rule

log = get_enterprise_logger("ApiSyncWorker")

def safe_string(value):
    if value is None: return ""
    val_str = str(value).strip()
    if val_str.lower() == "none": return ""
    return val_str

def safe_int_string(value):
    if value is None: return ""
    val_str = str(value).strip()
    if val_str.lower() == "none" or val_str == "": return ""
    if val_str.startswith('[') and val_str.endswith(']'): val_str = val_str.strip('[]')
    if ',' in val_str: val_str = val_str.split(',')[0].strip()
    try: return str(int(float(val_str)))
    except ValueError: return ""

def build_remote_study_payload(study_uid: str, is_update: bool = False):
    study = PatientStudy.query.filter_by(study_uid=study_uid).first()
    if not study: return None, []

    mapping = InstitutionMapping.query.filter_by(dicom_institution_name=study.institution_name).first()
    if mapping and mapping.center:
        raw_center_code = mapping.center.center_code
        try: my_center_id = int(raw_center_code)
        except ValueError: my_center_id = raw_center_code 
    else:
        center_setting = SystemSetting.query.filter_by(key='center_id').first()
        try: my_center_id = int(center_setting.value) if center_setting and center_setting.value else 0
        except (ValueError, TypeError): my_center_id = 0

    rule_id, official_desc = get_mapped_rule(study.study_description, study.modality)
    current_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

    safe_to_report_count = study.uploaded_image_count - study.api_synced_image_count
    if safe_to_report_count <= 0: return None, [] 
        
    fetch_limit = safe_to_report_count

    from core.database import ImageInstance, Series
    unreported_images = ImageInstance.query.join(Series).filter(
        Series.study_uid == study.study_uid,
        ImageInstance.is_api_synced == False
    ).order_by(ImageInstance.received_at).limit(fetch_limit).all()

    if not unreported_images and is_update: return None, [] 

    study_entity = {
        "studyGUID": None,
        "studyID": None,
        "studyInstanceUID": study.study_uid,
        "seriesCount": len(study.series),
        "studyDate": study.study_date,
        "accessionNumber": study.accession_number or None, 
        "refPhysicianID": None,
        "modality": study.modality,
        "instanceCount": None,
        "creationDate": current_time_str, 
        "updationDate": None,
        "manufacturer": study.manufacturer or None, 
        "manufacturerModelName": study.manufacturer_model_name or None, 
        "institutionName": study.institution_name or "",
        "centerID": my_center_id,  
        "noOfImage": study.image_count,
        "imageUploaded": study.uploaded_image_count,
        "uploadingStatus": 1 if study.uploaded_image_count > 0 else 0,
        "refPhysicianName": study.ref_physician_name or "",
        "studyDescriptionID": rule_id,
        "patient": {
            "patientID": study.patient_id,
            "numberOfPatientRelatedStudies": None,
            "patientName": study.patient_name,
            "sex": study.patient_sex,
            "birthDate": study.patient_birth_date or None, 
            "age": study.patient_age
        },
        "seriesEntity": []
    }

    incremental_series_dict = {}
    for img in unreported_images:
        s_uid = img.series_uid
        series = img.series
        if s_uid not in incremental_series_dict:
            incremental_series_dict[s_uid] = {
                "seriesInstanceUID": series.series_uid,
                "seriesNo": series.series_number,
                "seriesDescription": series.series_description or "",
                "creationDate": current_time_str,
                "ww": safe_string(img.window_width), 
                "wl": safe_string(img.window_center), 
                "sliceThickness": safe_string(img.slice_thickness),
                "noOfFrames": "",
                "fileName": None,
                "images": None,
                "imageEntity": []
            }
            
        img_dict = {
            "imageGUID": None,
            "imageInstanceUID": img.sop_uid, 
            "instanceNO": img.instance_number,
            "creationDate": current_time_str, 
            "dcmFileName": os.path.basename(img.file_path),
            "acquistionTime": safe_string(img.acquisition_time), 
            "sliceThickness": safe_string(img.slice_thickness),  
            "sliceLocation": safe_string(img.slice_location),
            "seriesInstanceUid": s_uid, 
            "sopinstanceUID": img.sop_uid,          
            "modality": study.modality,              
            "columns": safe_string(img.columns),
            "rows": safe_string(img.rows),
            "photometricInterpretation": safe_string(img.photometric_interpretation),
            "bitsAllocated": safe_string(img.bits_allocated),
            "bitsStored": safe_string(img.bits_stored),
            "pixelRepresentation": safe_string(img.pixel_representation),
            "samplesPerPixel": safe_string(img.samples_per_pixel),
            "pixelSpacing": safe_string(img.pixel_spacing),
            "imageOrientationPatient": safe_string(img.image_orientation_patient),
            "imagePositionPatient": safe_string(img.image_position_patient),
            "frameOfReferenceUID": safe_string(img.frame_of_reference_uid),
            "imageType": safe_string(img.image_type),
            "sopclassUID": safe_string(img.sop_class_uid),
            "highBit": safe_string(img.high_bit),
            "windowCenter": safe_int_string(img.window_center),
            "windowWidth": safe_int_string(img.window_width)
        }
        incremental_series_dict[s_uid]["imageEntity"].append(img_dict)

    study_entity["seriesEntity"] = list(incremental_series_dict.values())
    return {"studyRequestEntity": study_entity}, unreported_images

def send_study_to_api(study_uid: str, is_update: bool = False) -> bool:
    payload_result = build_remote_study_payload(study_uid, is_update=is_update)
    if not payload_result or not payload_result[0]: return False
    payload, unreported_images = payload_result
    
    headers = {"Content-Type": "application/json", "Connection": "close"}
    from core.database import get_system_setting
    API_URL = get_system_setting('CREATE_STUDY_API_URL', 'http://127.0.0.1:5005/api/create_study')
    if not API_URL or not API_URL.strip().startswith(('http://', 'https://')):
        log.warning(f"Skipping API sync: CREATE_STUDY_API_URL is unconfigured or invalid ('{API_URL}')")
        return False
    
    try:
        response = requests.post(API_URL.strip(), json=payload, headers=headers, timeout=10, verify=False)
        if response.status_code in (200, 201):
            response_data = response.json()
            if response_data.get("success", False):
                with db_lock:
                    for img in unreported_images: img.is_api_synced = True
                    study = db.session.get(PatientStudy, study_uid)
                    if study: study.api_synced_image_count += len(unreported_images)
                    db.session.commit()
                return True
            else:
                log.error(f"API rejected payload: {response_data}")
                return False
        else:
            log.error(f"API HTTP Error: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        log.error(f"Failure in send_study_to_api: {e}")
        return False

def sync_upload_count(study_uid: str, uploaded_count: int, total_count: int):
    from core.database import get_system_setting
    study = PatientStudy.query.filter_by(study_uid=study_uid).first()
    if not study:
        return False

    entity = {
        "studyInstanceUID": study_uid,
        "imageUploaded": uploaded_count,
        "noOfImage": total_count,
        "institutionName": study.institution_name or "",
        "uploadingStatus": 1 if uploaded_count >= total_count else 0
    }
    API_URL = get_system_setting('CREATE_STUDY_API_URL', 'http://127.0.0.1:5005/api/create_study')
    if not API_URL or not API_URL.strip().startswith(('http://', 'https://')):
        return False

    try:
        requests.post(API_URL.strip(), json={"studyRequestEntity": entity}, headers={"Content-Type": "application/json"}, timeout=5, verify=False)
        return True
    except Exception as e:
        log.error(f"Failed to sync count: {e}")
        return False

class ApiSyncWorkerService(threading.Thread):
    def __init__(self, app):
        super().__init__(name="ApiSyncWorkerService", daemon=True)
        self.app = app
        self._shutdown_flag = threading.Event()
    
    def shutdown(self):
        self._shutdown_flag.set()

    def run(self):
        log.info("API Sync worker thread started")
        while not self._shutdown_flag.is_set():
            with self.app.app_context():
                try:
                    pending = PatientStudy.query.filter(PatientStudy.api_synced_image_count == 0, PatientStudy.uploaded_image_count > 0).all()
                    for study in pending:
                        s_uid, uploaded, total = study.study_uid, study.uploaded_image_count, study.image_count
                        if send_study_to_api(s_uid, is_update=False):
                            try: sync_upload_count(s_uid, uploaded, total)
                            except Exception as e: log.error(f"Sync initial progress failed: {e}")        

                    incremental = PatientStudy.query.filter(PatientStudy.api_synced_image_count > 0, PatientStudy.uploaded_image_count > PatientStudy.api_synced_image_count).all()
                    for study in incremental: send_study_to_api(study.study_uid, is_update=True)

                except Exception as e:
                    db.session.rollback()
                    log.error(f"API Sync Loop Error: {e}")
                    
            for _ in range(50):
                if self._shutdown_flag.is_set(): break
                time.sleep(0.1)
