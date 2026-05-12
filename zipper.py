import os
import math
import zipfile
import concurrent.futures
from logger import get_enterprise_logger
import time

# Import shared tools
from database import db_lock, ModalityZipRule
# We import the Profile you already wrote in compressor.py
from systemProfile import SystemProfile 

log = get_enterprise_logger("ZipperEngine")

def create_single_zip_chunk(batch_files: list, zip_filepath: str) -> str:
    """Atomic worker: writes to .tmp, then renames to .zip"""
    tmp_filepath = zip_filepath + ".tmp"
    try:
        # ZIP_STORED is fast because it does zero compression math
        with zipfile.ZipFile(tmp_filepath, 'w', compression=zipfile.ZIP_STORED) as zipf:
            for file_path in batch_files:
                zipf.write(file_path, arcname=os.path.basename(file_path))
                time.sleep(0.01)
        
        os.replace(tmp_filepath, zip_filepath)
        return zip_filepath
    except Exception as e:
        log.error(f"Worker failure: {os.path.basename(zip_filepath)}", exc_info=True)
        if os.path.exists(tmp_filepath):
            try: os.remove(tmp_filepath)
            except OSError: pass
        return ""

def create_chunked_zips(staging_dir: str, outbound_base: str, study_uid: str, clean_name: str, modality: str) -> bool:
    """
    Hardware-Adaptive Zipper: 
    - 1 thread for old computers (No disk thrashing)
    - Up to 16 threads for modern computers (Maximum speed)
    """
    try:
        staged_files = [os.path.join(staging_dir, f) for f in os.listdir(staging_dir) 
                        if os.path.isfile(os.path.join(staging_dir, f))]
        
        if not staged_files:
            log.warning("No files to zip", extra={"study_uid": study_uid})
            return False

        with db_lock:
            rule = ModalityZipRule.query.filter_by(modality_name=modality).first()
            chunk_size = rule.max_files_per_zip if rule else 100
        
        total_chunks = math.ceil(len(staged_files) / chunk_size)
        
       
        # 1. PLAN THE TASKS
        tasks = []
        for i in range(total_chunks):
            chunk = staged_files[i * chunk_size : (i + 1) * chunk_size]
            file_count = len(chunk)
            
            # Grab the actual filename of the first DICOM in this specific chunk
            first_file_name = os.path.basename(chunk[0])
            
            # Build the exact requested string: "CT.1.2.3.dcm_contains_101_images.zip"
            z_name = f"{first_file_name}_contains_{file_count}_images.zip"
            
            tasks.append((chunk, os.path.join(outbound_base, z_name)))

        # 2. CALCULATE HARDWARE SPEED LIMIT
       
        # Let the centralized SystemProfile do the math for an IO-bound task
        workers = SystemProfile.get_optimal_threads(task_type="IO")
        optimal_workers = min(workers, total_chunks) # Never spawn more workers than chunks
        
        if optimal_workers == 1:
            log.warning("Zipper: LEGACY MODE. Sequential zipping engaged for safety.")
        else:
            log.info(f"Zipper: MODERN MODE. Running {optimal_workers} parallel workers.")

        # 3. EXECUTE PARALLEL WORK
        created_zips = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=optimal_workers) as executor:
            future_to_zip = {executor.submit(create_single_zip_chunk, t[0], t[1]): t[1] for t in tasks}
            
            for future in concurrent.futures.as_completed(future_to_zip):
                res = future.result()
                if res == "": return False # Worker crashed
                created_zips.append(res)

     
        
        return True

    except Exception:
        log.error("Zipper Engine critical failure", exc_info=True)
        return False