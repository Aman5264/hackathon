import json
import os
import pydicom
from typing import Dict, List, Any
from logger import get_enterprise_logger

log = get_enterprise_logger("MetadataEngine")

class MetadataEngine:
    _instance = None
    _rules: Dict[str, Dict[str, List[str]]] = {}
    _is_loaded: bool = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MetadataEngine, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not self._is_loaded:
            self.load_modality_rules()
            self.__class__._is_loaded = True

    def load_modality_rules(self) -> None:
        """Safely parses the JSON layout on boot with log validation."""
        config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'modality_rules.json')
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                self.__class__._rules = json.load(f)
            log.info("Successfully loaded modality_rules.json into memory.")
        except Exception as e:
            log.error(f"Failed to load modality_rules.json: {e}. Falling back to empty ruleset.", exc_info=True)
            self.__class__._rules = {}

    def extract_best_study_description(self, dataset: pydicom.dataset.Dataset) -> str:
        """
        Performs prioritized extraction using standard string sanitization.
        Skips meaningless data strings like 'CR', 'DX', 'XRAY', or empty values.
        """
        modality = str(getattr(dataset, "Modality", "")).strip().upper()
        manufacturer_raw = str(getattr(dataset, "Manufacturer", "")).strip().upper()

        modality_rules = self._rules.get(modality, {})
        
        # Upper-case fallback checking to match Manufacturer keys
        chosen_manufacturer = "DEFAULT"
        for mfg_key in modality_rules.keys():
            if mfg_key != "DEFAULT" and mfg_key in manufacturer_raw:
                chosen_manufacturer = mfg_key
                break

        tag_priority_list = modality_rules.get(chosen_manufacturer)
        
        # If no specific modality or manufacturer rules exist, we fallback to a hardcoded standard list.
        if not tag_priority_list:
             tag_priority_list = ["StudyDescription", "SeriesDescription", "PerformedProcedureStepDescription"]

        meaningless_strings = {"CR", "DX", "XRAY", "X-RAY", "US", "CT", "MR", "NM", "PT", "MG"}

        for tag_name in tag_priority_list:
            raw_val = getattr(dataset, tag_name, "")
            
            # Handle MultiValue or List types gracefully
            if isinstance(raw_val, list) or type(raw_val).__name__ == 'MultiValue':
                raw_val = " ".join([str(v) for v in raw_val])
            
            val = str(raw_val).strip(" \0\r\n")
            
            if not val:
                continue
                
            if val.upper() in meaningless_strings:
                continue

            patient_id = str(getattr(dataset, "PatientID", "UNKNOWN"))
            log.info(f"Extracted study description from tag '{tag_name}' for Patient ID: {patient_id}")
            return val

        # Absolute fallback if all extraction fails
        return ""

# Expose a ready-to-use singleton instance
engine = MetadataEngine()
