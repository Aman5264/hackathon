import os
import sys
import shutil
import subprocess
import pydicom
from logger import get_enterprise_logger

log = get_enterprise_logger("Compressor")


def get_binary_path(binary_name: str) -> str:
    """
    Dynamically finds the path to external binaries.
    Works whether running as a .py script or a compiled PyInstaller .exe.
    """
    if getattr(sys, 'frozen', False):
        # We are running as a compiled .exe
        base_dir = os.path.dirname(sys.executable)
    else:
        # We are running as a normal python script
        base_dir = os.path.dirname(os.path.abspath(__file__))
        
    return os.path.abspath(os.path.join(base_dir, 'tools', binary_name))


def get_compression_settings(modality: str):
    """
    Fetches the specific compression rule for a modality from the database. 
    Defaults to highly-safe LOSSLESS if no rule exists.
    """
    clean_modality = str(modality).strip().upper()
    try:
        from database import db_lock, ModalityCompressionRule
        with db_lock:
            rule = ModalityCompressionRule.query.filter_by(modality_name=clean_modality).first()
            if rule:
                return rule.compression_type, rule.compression_level
    except Exception as e:
        log.error(f"Failed to fetch compression rule for {clean_modality}: {e}")
        
    return 'LOSSLESS', 100  # Enterprise Safe Default


def compress_single_dicom(input_path: str, output_path: str, modality: str) -> bool:
    """
    Executes DCMTK (dcmcjpeg.exe) to compress the file based on Database rules.
    """
    if not os.path.exists(input_path):
        return False

    comp_type, quality = get_compression_settings(modality)
    filename = os.path.basename(input_path)

    # =====================================================================
    # LANE 1: PURE COPY (No processing overhead)
    # =====================================================================
    if comp_type == 'COPY':
        try:
            shutil.copy2(input_path, output_path)
            return True
        except Exception as e:
            log.error(f"Copy failed for {os.path.basename(input_path)}: {e}")
            return False

    # =====================================================================
    # PRE-CHECK: Prevent Double-Compression
    # =====================================================================
    try:
        # Fast read: We ONLY read the metadata, skipping the massive pixel array
        ds = pydicom.dcmread(input_path, stop_before_pixels=True)
        if ds.file_meta.TransferSyntaxUID.is_compressed:
            shutil.copy2(input_path, output_path)
            return True
    except Exception:
        pass # If we can't read the metadata, we just pass it to DCMTK to handle

    # =====================================================================
    # LANE 2 & 3: DCMTK COMPRESSION (LOSSLESS or LOSSY)
    # =====================================================================
    dcmtk_exe = get_binary_path("dcmcjpeg.exe")
    
    if not os.path.exists(dcmtk_exe):
        log.critical(f"🚨 Missing required binary at: {dcmtk_exe}. Falling back to COPY.")
        shutil.copy2(input_path, output_path)
        return True

   
    # Use a temporary output file so partial writes don't corrupt the pipeline
    temp_out = output_path + ".tmp"
    
    cmd = [dcmtk_exe]
    
    if comp_type == 'LOSSLESS':
        # 🚨 THE FIX 1: '+e1' is the exact DCMTK flag for Lossless JPEG (Process 14 SV1)
        cmd.append("+e1") 
    else:
        # 🚨 THE FIX 2: '+ee' (12-bit safe Lossy) and '+q' (Quality, MUST be a plus sign)
        cmd.extend(["+ee", "+q", str(max(10, min(100, quality)))]) 
        
    cmd.extend([input_path, temp_out])

    try:
        # Execute the binary
        result = subprocess.run(
            cmd, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE, 
            timeout=20, # 20 second hard limit per file
            text=True,
            # Windows ONLY: Prevents the black CMD box from flashing on the screen
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0
        )

        if result.returncode == 0 and os.path.exists(temp_out):
            # Success! Rename the temp file to the final zip-ready file
            os.rename(temp_out, output_path)
            log.info(f"✅ DCMTK Success ({comp_type}): {filename}")
            return True
        else:
            log.error(f"❌ DCMTK Failed for {os.path.basename(input_path)}: {result.stderr}")
            
            # 🛡️ FALLBACK: If DCMTK crashes, just copy the file so the pipeline doesn't freeze
            if os.path.exists(temp_out):
                os.remove(temp_out)
            shutil.copy2(input_path, output_path)
            return True

    except subprocess.TimeoutExpired:
        log.error(f"⏱️ DCMTK timeout on {os.path.basename(input_path)}")
        
        # Kill the stuck/zombie DCMTK process aggressively
        if os.name == 'nt':
            subprocess.run(['taskkill', '/F', '/IM', 'dcmcjpeg.exe'], capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
            
        if os.path.exists(temp_out):
            os.remove(temp_out)
            
        # 🛡️ FALLBACK: Copy original
        shutil.copy2(input_path, output_path)
        return True
        
    except Exception as e:
        log.error(f"Compression system error: {e}")
        shutil.copy2(input_path, output_path)
        return True