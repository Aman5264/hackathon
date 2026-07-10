import requests
import datetime
from config import Config
from core.database import db, DescriptionRule, DescriptionMapping, SystemSetting
from logger import get_enterprise_logger

log = get_enterprise_logger("MetadataMapper")

def sync_rules_to_database():
    """
    Downloads rules from the central API.
    Adds new active rules and DELETES any rules that have been marked as 'D' (Deactivated).
    """
    # 1. Check the database for the last sync date
    last_sync = SystemSetting.query.filter_by(key='last_rule_sync_date').first()
    
    if last_sync and last_sync.value:
        study_date = last_sync.value
        log.info(f"🔄 Incremental Sync: Fetching rules from {study_date}...")
    else:
        study_date = "2000-01-01"
        log.info("🔄 Initial Full Sync: Fetching all historical rules...")

    payload = {
        "studyRequestEntity": {
            "modality": "CT,CR,DX,MR,US,XA,NM,PET",          
            "creationDate": Config.Desc_S_date     
        }
    }

    headers = {"Content-Type": "application/json"}

    try:
        response = requests.post(Config.DESCRIPTION_API_URL, json=payload, headers=headers, timeout=15)
        
        if response.status_code == 200:
            api_data = response.json()
            rules_list = api_data.get("responseList", [])
            
            if rules_list:
                new_rules_count = 0
                deleted_rules_count = 0
                
                for rule in rules_list:
                    rule_id = rule.get("studyDescriptionID")
                    desc = rule.get("studyDescription", "")
                    modality = rule.get("modality", "")
                    status = rule.get("status", "") 
                    
                    if not rule_id or not desc:
                        continue

                    # Ask the DB if we already have this rule
                    existing_rule = DescriptionRule.query.filter_by(rule_id=rule_id).first()
                    
                    # --- DELETION LOGIC (If the hospital marked it 'D') ---
                    if status == "D":
                        if existing_rule:
                            # 1. Delete any custom mappings attached to this rule first to avoid DB crashes
                            DescriptionMapping.query.filter_by(rule_id=rule_id).delete()
                            
                            # 2. Delete the official rule itself
                            db.session.delete(existing_rule)
                            deleted_rules_count += 1
                            
                        continue # Skip to the next rule in the list

                    # --- INSERT LOGIC (For Active Rules) ---
                    if not existing_rule:
                        new_rule = DescriptionRule(
                            rule_id=rule_id,
                            original_description=desc,
                            modality=modality
                        )
                        db.session.add(new_rule)
                        new_rules_count += 1
                
                log.info(f"✅ Sync Complete: Added {new_rules_count} new rules, Deleted {deleted_rules_count} deactivated rules.")
            else:
                log.info("✅ Sync Complete: Dictionary is already up-to-date (0 changes).")

            # Update the Last Sync Date to TODAY
            today_str = datetime.datetime.now().strftime("%Y-%m-%d")
            
            if not last_sync:
                new_setting = SystemSetting(key='last_rule_sync_date', value=today_str)
                db.session.add(new_setting)
            else:
                last_sync.value = today_str
                
            db.session.commit()
            log.info(f"📅 System setting 'last_rule_sync_date' updated to: {today_str}")

        else:
            log.error(f"❌ Sync Failed: API returned HTTP {response.status_code}")

    except Exception as e:
        log.error(f"CRITICAL: Failed to sync description rules to database: {e}")


def get_mapped_rule(machine_description: str, modality: str):
    """
    1. Checks if there are custom mappings (supports multiple: X -> A, B).
    2. If not, checks for an exact match (A -> A).
    Returns a tuple: (comma_separated_rule_ids, comma_separated_descriptions)
    """
    if not machine_description:
        return None, None

    # STEP 1: Look in the custom Mapping Table using .all()
    custom_maps = DescriptionMapping.query.filter_by(machine_description=machine_description).all()
    
    if custom_maps:
        # THE FIX: Remove 'if m.rule' from the ID extraction. 
        # Always send the ID if it exists in the mapping table!
        rule_ids = ",".join([str(m.rule_id) for m in custom_maps if m.rule_id])
        
        # We still safely try to grab the text descriptions if they exist locally
        official_descs = ", ".join([m.rule.original_description for m in custom_maps if m.rule])
        
        if rule_ids:
            log.info(f"🔗 Custom Map Found: '{machine_description}' linked to Rules [{rule_ids}]")
            return rule_ids, official_descs

    # STEP 2: Fallback to Exact Match in the Rules Table (Now supports multiple!)
    exact_rules = DescriptionRule.query.filter_by(
        original_description=machine_description, 
        modality=modality
    ).all()

    if exact_rules:
        # Join multiple IDs and Descriptions with commas
        rule_ids = ",".join([str(r.rule_id) for r in exact_rules])
        official_descs = ", ".join([r.original_description for r in exact_rules])
        
        log.info(f"✅ Exact Match Found: Rules [{rule_ids}]")
        return rule_ids, official_descs
        
    log.warning(f"❌ No mapping or rule found for '{machine_description}'")
    return None, None


def get_official_description(machine_description: str, modality: str) -> str:
    # Using an underscore '_' tells Pylance to safely ignore the first returned value
    _, official_descs = get_mapped_rule(machine_description, modality)
    return official_descs
