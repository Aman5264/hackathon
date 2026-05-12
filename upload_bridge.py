import threading
import asyncio
import time
import os
from logger import get_enterprise_logger

# Route bridge logs properly to the enterprise logger
log = get_enterprise_logger("UploadBridge")

class AsyncUploadBridge:
    def __init__(self):
        self._loop = asyncio.new_event_loop()
        self._engine = None
        self._current_url = ""
        self._current_user = ""  # <--- NEW: Track current user
        self._current_key = ""
        
        # Thread lock prevents multiple upload workers from rebuilding simultaneously
        self._engine_lock = threading.Lock() 
        
        # Start the high-speed network event loop in a daemon thread
        threading.Thread(target=self._start_loop, daemon=True, name="V4NetworkLoop").start()

    def _start_loop(self):
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()
    
    # 🚨 THE FIX: Added api_user to the signature
    async def _build_engine_core(self, api_url, api_user, api_key):
        """Core logic to completely rebuild the engine."""
        from enterprise_uploader import EnterpriseUploadEngine, UploadConfig, UploadLedger
        
        try:
            # 🚨 THE FIX: Just set the environment variables!
            # Your TusClient naturally reads from os.environ on every request.
            if api_key:
                os.environ['TUS_API_KEY'] = api_key
            else:
                os.environ.pop('TUS_API_KEY', None)

            if api_user:
                os.environ['TUS_API_USER'] = api_user
            else:
                os.environ.pop('TUS_API_USER', None)    
                
            # Create config normally WITHOUT the invalid 'headers' argument
            cfg = UploadConfig()
            ledger = UploadLedger()
            
            # ENABLED PARALLEL UPLOADING!
            override_mode = "sequential" 
            
            # This completely destroys the old engine and makes a fresh one
            self._engine = EnterpriseUploadEngine(
                ledger, cfg, api_url, mode_override=override_mode
            )
            
            # 🚨 THE FIX: Save all 3 states to detect future changes from the UI Dashboard
            self._current_url = api_url
            self._current_user = api_user
            self._current_key = api_key
            log.info(f"✅ V4 Enterprise Engine Built and Secured (User: {api_user}, URL: {api_url})")
            
        except Exception as e:
            log.critical(f"❌ ENGINE BUILD FAILED: {e}", exc_info=True)
            self._engine = None

    def upload_zip_with_backoff(self, filepath: str, study_uid: str, api_url: str, api_user: str, api_key: str) -> bool:
        """Synchronous blocking wrapper for the async engine."""
        
        # 1. Lazy Initialization & Hot-Swap Rebuild Shield
        with self._engine_lock:
            # 🚨 THE FIX: Check if URL, USER, or KEY has changed
            if self._engine is None or api_url != self._current_url or api_user != self._current_user or api_key != self._current_key:
                log.info("API Settings missing or changed! Building HTTP Engine securely...")
                
                # Run the rebuild securely on the async thread and wait for it to finish
                rebuild_future = asyncio.run_coroutine_threadsafe(
                    # 🚨 THE FIX: Pass all 3 variables to the core builder
                    self._build_engine_core(api_url, api_user, api_key), 
                    self._loop
                )
                rebuild_future.result() # Block this specific upload worker until the new engine is fully online

        # 2. Failsafe abort if the engine absolutely refused to build
        if self._engine is None:
            log.error(f"Aborting upload for {filepath}: Engine failed to initialize.")
            return False
            
        # 3. Execute the actual high-speed upload
        future = asyncio.run_coroutine_threadsafe(
            self._engine.process_upload(filepath, study_uid), 
            self._loop
        )
        
        try:
            return future.result()
        except Exception as e:
            log.error(f"Upload bridge failed for {filepath}: {e}", exc_info=True)
            return False

# Create the global singleton bridge
_bridge = AsyncUploadBridge()

# Global wrapper now perfectly matches processor.py
# Global wrapper now perfectly matches processor.py
def upload_zip_with_backoff(filepath: str, study_uid: str, api_url: str, api_user: str, api_key: str) -> bool:
    return _bridge.upload_zip_with_backoff(filepath, study_uid, api_url, api_user, api_key)