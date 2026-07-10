import threading

class ActivityTracker:
    """
    A Thread-Safe Singleton designed to hold the current real-time activity 
    of the background backend workers.
    """
    _instance = None
    _creation_lock = threading.Lock() # Protects the actual creation of the Singleton

    def __new__(cls):
        # 1. The Singleton Logic
        with cls._creation_lock:
            if cls._instance is None:
                cls._instance = super(ActivityTracker, cls).__new__(cls)
                # Initialize the actual data storage and its lock
                cls._instance._message = "System Idle... Waiting for DICOM data."
                cls._instance._data_lock = threading.Lock()
        return cls._instance

    # 2. The Safe Setter
    def set_activity(self, message: str):
        with self._data_lock:
            self._message = message

    # 3. The Safe Getter
    def get_activity(self) -> str:
        with self._data_lock:
            return self._message

# Instantiate it once here so other files can just import 'tracker'
tracker = ActivityTracker()
