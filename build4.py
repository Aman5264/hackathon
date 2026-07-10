import os
import subprocess
import sys

def build_app():
    print("🚀 Starting Nuitka One-Dir Build Process...")
    
    # Pre-Flight Check for the tools folder!
    if not os.path.exists("tools") or not os.listdir("tools"):
        print("⚠️ WARNING: Your 'tools' folder is empty or missing!")
        print("Ensure 'dcmcjpeg.exe' is inside a folder named 'tools' before distributing.")
        print("-" * 50)
        
    # Pre-Flight Check for the assets folder!
    if not os.path.exists("assets/icon.ico"):
        print("⚠️ WARNING: 'assets/icon.ico' not found. Make sure the path is correct!")
        print("-" * 50)
    
    cmd = [
        sys.executable, "-m", "nuitka",
        "--standalone",               
        "--assume-yes-for-downloads", 
        
        # ==========================================
        # 1. UI & OS CONFIGURATION
        # ==========================================
        "--windows-console-mode=disable",          
        "--windows-icon-from-ico=assets/icon.ico",
        "--include-data-file=modality_rules.json=modality_rules.json", 
        
        # --- METADATA FLAGS ---
        "--company-name=Icare Infosystem Pvt. Ltd.",
        "--product-name=Icare DICOM Uploader",
        "--file-version=2.0.0.0",
        "--product-version=1.0.0.0",
        "--copyright=Copyright (c) 2026 Icare Infosystem",
        
        # ==========================================
        # 2. PLUGINS & COMPILE BYPASSES
        # ==========================================
        "--enable-plugin=anti-bloat",
        "--no-deployment-flag=excluded-module-usage",  
        
        # 🚨 THE ULTIMATE OVERRIDE: Shut off Nuitka's auto-detect so it stops conflicting with us
        "--disable-plugin=pywebview", 
        
        # ==========================================
        # 3. EXTERNAL DATA & ASSETS
        # ==========================================
        "--include-data-dir=tools=tools",          
        "--include-data-dir=templates=templates",  
        "--include-data-dir=assets=assets",        
        "--include-data-file=.env=.env",           
        
        # ==========================================
        # 4. HIDDEN IMPORTS & CORE PACKAGES
        # ==========================================
        "--include-package=sqlalchemy",
        "--include-package=flask",
        "--include-package=pynetdicom",
        "--include-package=pydicom",
        "--include-module=waitress",
        "--include-module=pythonjsonlogger",
        "--include-package-data=pydicom",
        "--include-package-data=pynetdicom",
        
        # ==========================================
        # 5. SURGICAL PYWEBVIEW DEPENDENCY INCLUSION
        # ==========================================
        "--include-module=webview",
        "--include-module=webview.util",
        "--include-module=webview.menu",
        "--include-module=webview.platforms",
        "--include-module=webview.platforms.win32",    
        "--include-module=webview.platforms.winforms", 
        "--include-module=webview.platforms.edgechromium",
        
        # ==========================================
        # 6. ENTRY POINT
        # ==========================================
        "app.py"
    ]

    print("Executing Nuitka build...")
    print("-" * 50)
    
    result = subprocess.run(cmd)

    print("-" * 50)
    if result.returncode == 0:
        print("\n✅ Build Successful!")
        print("IMPORTANT: Your application is the ENTIRE 'app.dist' folder!")
        print("Do not separate the .exe from the .dist folder. Distribute the whole folder.")
    else:
        print("\n❌ Build Failed. Check the terminal errors above.")

if __name__ == "__main__":
    build_app()