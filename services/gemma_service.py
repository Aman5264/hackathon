import os
import json
import time
from datetime import datetime
from typing import Dict, Any, Optional

from config import Config
from logger import get_enterprise_logger

log = get_enterprise_logger("GemmaService")

class GemmaService:
    """
    Radnova Gemma Report Generation Service.
    Produces structured preliminary radiology draft findings.
    
    Supports:
    1. Real Gemma / Gemini API via configured server/key.
    2. Clearly labelled Synthetic-Findings Demo Mode when offline, keyless,
       or when multimodal DICOM pixel analysis is not configured.
    """
    
    @classmethod
    def generate_report(cls, study_uid: str, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Generates a structured draft findings report for a study."""
        log.info(f"Generating Gemma draft report for study: {study_uid}")
        
        # 1. Gather context
        context = cls._gather_context(study_uid, metadata)
        modality = context.get('modality', 'UNKNOWN').upper()
        description = context.get('study_description', '') or f"{modality} Examination"
        
        api_key = getattr(Config, 'GEMMA_API_KEY', '') or os.environ.get('GEMINI_API_KEY', '')
        api_url = getattr(Config, 'GEMMA_API_URL', '')
        demo_mode = getattr(Config, 'GEMMA_DEMO_MODE', True) or (not api_key and not api_url)
        
        report: Optional[Dict[str, Any]] = None
        
        # 2. Try Live API if configured and not explicitly demo mode
        if not demo_mode and (api_key or api_url):
            try:
                report = cls._call_gemma_api(context, api_key, api_url)
            except Exception as e:
                log.warning(f"Live Gemma API call failed ({e}); falling back to synthetic findings demo mode.")
                report = None
                
        # 3. Fallback to Structured Demo Findings if needed
        if report is None:
            report = cls._generate_synthetic_demo_findings(context)
            
        # 4. Save report artifact to disk alongside archive
        try:
            report_filename = f"RADNOVA_{modality}_{study_uid[:8]}_report.json"
            report_path = os.path.join(Config.ARCHIVE_FOLDER, report_filename)
            with open(report_path, "w", encoding="utf-8") as f:
                json.dump(report, f, indent=2)
            report["local_report_path"] = report_path
            log.info(f"Saved Gemma report draft to: {report_path}")
        except Exception as e:
            log.warning(f"Could not write report JSON to disk: {e}")
            
        # 5. Queue update to DB
        try:
            from core.event_bus import db_write_queue, DBWriteEvent
            db_write_queue.put(DBWriteEvent(
                action="update_study_report",
                payload={
                    "study_uid": study_uid,
                    "report_status": report.get("report_status", "DRAFT"),
                    "report_json": json.dumps(report)
                }
            ))
        except Exception as e:
            log.warning(f"Failed to queue study report DB update: {e}")
            
        return report

    @classmethod
    def _gather_context(cls, study_uid: str, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Gathers available metadata from DB or payload."""
        ctx = {
            "study_uid": study_uid,
            "patient_id": "Unknown",
            "patient_name": "De-identified",
            "patient_age": "N/A",
            "patient_sex": "O",
            "modality": "CT",
            "study_description": "",
            "study_date": datetime.now().strftime("%Y-%m-%d"),
            "image_count": 0,
            "manufacturer": "Standard Modality"
        }
        if metadata:
            ctx.update(metadata)
            return ctx
            
        try:
            from core.database import db, PatientStudy
            study = db.session.get(PatientStudy, study_uid)
            if study:
                ctx["patient_id"] = study.patient_id or "De-identified"
                ctx["patient_name"] = study.patient_name or "De-identified"
                ctx["patient_age"] = study.patient_age or "N/A"
                ctx["patient_sex"] = study.patient_sex or "O"
                ctx["modality"] = study.modality or "CT"
                ctx["study_description"] = study.study_description or ""
                ctx["study_date"] = study.study_date or datetime.now().strftime("%Y-%m-%d")
                ctx["image_count"] = study.image_count or 0
                ctx["manufacturer"] = study.manufacturer or "Standard Modality"
        except Exception as e:
            log.debug(f"Could not read study from DB for Gemma context: {e}")
            
        return ctx

    @classmethod
    def _generate_synthetic_demo_findings(cls, ctx: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generates realistic, clearly labelled synthetic draft findings for demo purposes.
        Adheres to requirements: Never presents synthetic data as genuine image interpretation.
        """
        modality = ctx.get("modality", "CT").upper()
        desc = ctx.get("study_description", "").strip() or f"{modality} Scan"
        image_count = ctx.get("image_count", 1)
        age = ctx.get("patient_age", "N/A")
        sex = ctx.get("patient_sex", "O")
        
        # Modality-specific clinical template
        if modality in ("CR", "DX"):
            technique = f"Single or multi-view digital radiography ({modality}) of {desc}. Total images: {image_count}."
            findings = (
                "The cardiothoracic silhouette and mediastinal contours are within normal limits for age. "
                "Lungs are clear bilaterally with no focal consolidation, pleural effusion, or pneumothorax. "
                "Osseous structures and thoracic soft tissues appear intact with no acute displaced fractures."
            )
            impression = (
                "No acute cardiopulmonary disease identified. Clear bilateral lung fields."
            )
        elif modality == "CT":
            technique = f"Axial computed tomography ({modality}) slices evaluated for {desc}. Acquired frames: {image_count}."
            findings = (
                "Cross-sectional examination demonstrates symmetric anatomical landmarks. "
                "No mass effect, midline shift, abnormal fluid collections, or acute territorial infarctions visualized. "
                "Visualized osseous structures demonstrate intact cortical architecture without aggressive lytic or blastic lesions."
            )
            impression = (
                "Unremarkable preliminary CT evaluation without acute gross abnormality. Correlate with clinical indications."
            )
        elif modality == "MR":
            technique = f"Magnetic Resonance Imaging ({modality}) protocol for {desc}. Series volume: {image_count} images."
            findings = (
                "Multiplanar, multisequence images demonstrate expected signal intensity across parenchymal/musculoskeletal compartments. "
                "Ventricular and cistern spaces unremarkable. No pathologic restricted diffusion or abnormal focal mass."
            )
            impression = (
                "Preliminary MRI draft review: No gross structural disruption or acute intracranial/soft-tissue lesion identified."
            )
        elif modality == "US":
            technique = f"Real-time diagnostic ultrasound ({modality}) for {desc}. Images captured: {image_count}."
            findings = (
                "Normal acoustic transmission and architectural contours demonstrated. "
                "No pathologic cystic or solid mass identified. Visualized vasculature demonstrates expected luminal patency."
            )
            impression = (
                "Unremarkable targeted ultrasound examination. Correlate with real-time dynamic operator assessment."
            )
        else:
            technique = f"Diagnostic {modality} examination for {desc} ({image_count} instances)."
            findings = (
                "Technical factors verified. Standard anatomical alignment noted without macroscopic structural distortion."
            )
            impression = (
                f"Preliminary draft assessment for {modality} {desc}. No acute contraindications documented."
            )
            
        limitations = (
            f"Automated draft report generated in Radnova Demo Mode without pixel-level computer-vision verification. "
            f"Assessment is bounded by available study tags (Modality: {modality}, Description: {desc}, Count: {image_count}). "
            f"Clinical history and prior comparative studies were not integrated."
        )
        
        return {
            "study_uid": ctx.get("study_uid"),
            "modality": modality,
            "patient_context": {
                "patient_id": ctx.get("patient_id"),
                "patient_name": ctx.get("patient_name"),
                "age": age,
                "sex": sex
            },
            "technique": technique,
            "findings": findings,
            "impression": impression,
            "limitations_and_uncertainty": limitations,
            "report_status": "DRAFT",
            "is_synthetic_demo": True,
            "model": "Gemma-2-9B-IT (Synthetic Draft Mode)",
            "requires_radiologist_review": True,
            "mandatory_disclaimer": "AI PRELIMINARY DRAFT REPORT — REQUIRES MANDATORY RADIOLOGIST REVIEW AND SIGN-OFF. NOT FOR INDEPENDENT DIAGNOSIS.",
            "generated_at": datetime.now().isoformat()
        }

    @classmethod
    def _call_gemma_api(cls, ctx: Dict[str, Any], api_key: str, api_url: str) -> Dict[str, Any]:
        """Calls external Google Gemini / Gemma API with medical structured prompt."""
        import requests
        
        endpoint = api_url or f"https://generativelanguage.googleapis.com/v1beta/models/{getattr(Config, 'GEMMA_MODEL', 'gemma-2-9b-it')}:generateContent"
        
        system_prompt = (
            "You are a medical AI assistant helping a radiologist draft a preliminary findings report. "
            "Based on the provided DICOM study parameters, output ONLY a valid JSON object with keys: "
            "'technique', 'findings', 'impression', 'limitations_and_uncertainty', 'report_status' ('DRAFT'). "
            "Do not fabricate definitive pathology when pixels are uninspected; note uncertainty where appropriate."
        )
        
        user_prompt = (
            f"Study Modality: {ctx.get('modality')}\n"
            f"Procedure Description: {ctx.get('study_description')}\n"
            f"Patient Age/Sex: {ctx.get('patient_age')} / {ctx.get('patient_sex')}\n"
            f"Image Count: {ctx.get('image_count')}\n"
            f"Manufacturer: {ctx.get('manufacturer')}\n"
        )
        
        headers = {"Content-Type": "application/json"}
        params = {}
        if api_key and "generativelanguage.googleapis.com" in endpoint:
            params["key"] = api_key
        elif api_key:
            headers["Authorization"] = f"Bearer {api_key}"
            
        payload = {
            "contents": [
                {
                    "parts": [
                        {"text": f"{system_prompt}\n\n{user_prompt}"}
                    ]
                }
            ],
            "generationConfig": {
                "temperature": 0.2,
                "responseMimeType": "application/json"
            }
        }
        
        response = requests.post(endpoint, json=payload, headers=headers, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        # Parse output text
        candidate_text = data["candidates"][0]["content"]["parts"][0]["text"]
        parsed = json.loads(candidate_text)
        
        parsed["study_uid"] = ctx.get("study_uid")
        parsed["modality"] = ctx.get("modality")
        parsed["report_status"] = "DRAFT"
        parsed["model"] = getattr(Config, 'GEMMA_MODEL', 'gemma-2-9b-it')
        parsed["requires_radiologist_review"] = True
        parsed["is_synthetic_demo"] = False
        parsed["mandatory_disclaimer"] = "AI PRELIMINARY DRAFT REPORT — REQUIRES MANDATORY RADIOLOGIST REVIEW AND SIGN-OFF."
        parsed["generated_at"] = datetime.now().isoformat()
        
        return parsed
