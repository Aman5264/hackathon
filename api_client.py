import os
import requests
from database import db, PatientStudy, SystemSetting, DescriptionMapping, DescriptionRule, InstitutionMapping
from logger import get_enterprise_logger
from config import Config
import json

from datetime import datetime

log = get_enterprise_logger("API_Client")

def get_mapped_rule(machine_desc: str, modality: str):
    """
    Looks up official rules. Updated to support multiple comma-separated rules.
    """
    if not machine_desc: 
        return None, None
        
    # 1. Check for custom mappings (Can be multiple!)
    custom_maps = DescriptionMapping.query.filter_by(machine_description=machine_desc).all()
    if custom_maps:
        # Join multiple IDs and Descriptions with commas
        rule_ids = ",".join([m.rule_id for m in custom_maps if m.rule])
        official_descs = ", ".join([m.rule.original_description for m in custom_maps if m.rule])
        
        if rule_ids: 
            return rule_ids, official_descs
            
    # 2. Exact match fallback (Now supports multiple!)
    exact_rules = DescriptionRule.query.filter_by(
        original_description=machine_desc, 
        modality=modality
    ).all()

    if exact_rules: 
        # Join multiple IDs and Descriptions with commas
        rule_ids = ",".join([str(r.rule_id) for r in exact_rules])
        official_descs = ", ".join([r.original_description for r in exact_rules])
        return rule_ids, official_descs
        
    return None, None

def safe_string(value):
    """Catches both the Python None object AND the literal string 'None'."""
    if value is None:
        return ""
    
    val_str = str(value).strip()
    
    # If the word is "None", return a true empty string
    if val_str.lower() == "none":
        return ""
        
    return val_str
def safe_int_string(value):
    """
    Safely extracts a single integer string from DICOM MultiValues.
    Converts "[50, 50]" -> "50" to prevent Java Jackson crashes.
    """
    if value is None:
        return ""
        
    val_str = str(value).strip()
    if val_str.lower() == "none" or val_str == "":
        return ""
        
    # Remove Python list brackets if they exist
    if val_str.startswith('[') and val_str.endswith(']'):
        val_str = val_str.strip('[]')
        
    # If there are multiple values separated by a comma, grab only the first one
    if ',' in val_str:
        val_str = val_str.split(',')[0].strip()
        
    # Ensure it's a clean integer (handles decimal strings like "50.5" -> "50")
    try:
        return str(int(float(val_str)))
    except ValueError:
        return "" # If it's completely unparseable text, return empty
    
    
def build_remote_study_payload(study_uid: str, is_update: bool = False):
    study = PatientStudy.query.filter_by(study_uid=study_uid).first()
    if not study: return None, []

    # 1. Map the Center ID
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

    # =================================================================
    # 🚨 PAGINATION FIX: Calculate safe limits based on what is uploaded
    # =================================================================
    safe_to_report_count = study.uploaded_image_count - study.api_synced_image_count
    if safe_to_report_count <= 0:
        return None, [] 
        
    fetch_limit = safe_to_report_count

    from database import ImageInstance, Series
    unreported_images = ImageInstance.query.join(Series).filter(
        Series.study_uid == study.study_uid,
        ImageInstance.is_api_synced == False
    ).order_by(ImageInstance.received_at).limit(fetch_limit).all()

    if not unreported_images and is_update:
        return None, [] 

    # 2. MATCH THE EXACT STUDY LEVEL STRUCTURE 
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
        # "studyDescription": official_desc,
        # "history": study.patient_history or "", 
        
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

    # =================================================================
    # 3. DYNAMIC SERIES GROUPING 
    # =================================================================
    incremental_series_dict = {}

    for img in unreported_images:
        s_uid = img.series_uid
        series = img.series
        
        if s_uid not in incremental_series_dict:
            incremental_series_dict[s_uid] = {
                "seriesInstanceUID": series.series_uid,
                "seriesNo": series.series_number,
                "seriesDescription": series.series_description or "",
                "creationDate": None,
                "ww": safe_string(img.window_width), 
                "wl": safe_string(img.window_center), 
                "sliceThickness": safe_string(img.slice_thickness), 
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
    payload = {"studyRequestEntity": study_entity}

    return payload, unreported_images


def send_study_to_api(study_uid: str, is_update: bool = False) -> bool:
    # 1. Unpack the tuple from the builder
    payload_result = build_remote_study_payload(study_uid, is_update=is_update)
    if not payload_result or not payload_result[0]:
        return False
        
    payload, unreported_images = payload_result
    
    # 🚨 SSL CRASH FIX: Force connection close
    headers = {
        "Content-Type": "application/json",
        "Connection": "close"
    }
    
    from database import get_system_setting
    API_URL = get_system_setting(
        'CREATE_STUDY_API_URL', 
        'https://icare.anikrafoundation.com:8443/weasisintegratorwf/studymasterservice/createStudy'
    )
    
    try:
        log.info(f"{'INCREMENTAL' if is_update else 'INITIAL'} API Sync: Pushing {len(unreported_images)} new images for {study_uid}")
        
        # 🚨 SSL CRASH FIX: Added timeout=15 and verify=False
        response = requests.post(API_URL, json=payload, headers=headers, timeout=60, verify=False)
        
        if response.status_code in (200, 201):
            response_data = response.json()
            if response_data.get("success", False):
                
                # ==========================================================
                # 🚨 MASTER TRACKER FIX: Maintain Pagination Accuracy!
                # ==========================================================
                from database import db, db_lock, PatientStudy
                with db_lock:
                    for img in unreported_images:
                        img.is_api_synced = True
                        
                    study = PatientStudy.query.get(study_uid)
                    if study:
                        study.api_synced_image_count += len(unreported_images)
                        
                    db.session.commit()
                    
                return True
            else:
                log.error(f"API rejected the payload: {response_data}")
                return False
        else:
            log.error(f"API HTTP Error: {response.status_code} - {response.text}")
            return False

    except Exception as e:
        log.error(f"Critical failure in send_study_to_api: {e}", exc_info=True)
        return False

def sync_upload_count(study_uid: str, uploaded_count: int, total_count: int):
    """Pushes only the live uploaded_image_count to the Central API."""
    import requests
    from database import get_system_setting
    study = PatientStudy.query.filter_by(study_uid=study_uid).first()
    entity = {
        "studyInstanceUID": study_uid,
        "imageUploaded": uploaded_count, # 🚨 Uses the pure integer passed from the thread
        "institutionName": study.institution_name or "",
        "uploadingStatus": 1 if uploaded_count >= total_count else 0
    }
    payload = {"studyRequestEntity": entity}

    update_api_url = get_system_setting(
        'CREATE_STUDY_API_URL', 
        'https://icare.anikrafoundation.com:8443/weasisintegratorwf/studymasterservice/createStudy'
    )
    headers = {"Content-Type": "application/json"}
    
    try:
        # Fire and forget. 5 second timeout so it doesn't block the background uploader.
        requests.post(update_api_url, json=payload, headers=headers, timeout=5, verify=False)
        log.info(f"Updated live upload count on Central Server for {study_uid} -> {uploaded_count}/{total_count}")
        return True
    except Exception as e:
        log.error(f"Failed to sync upload count for {study_uid}: {e}")
        return False
