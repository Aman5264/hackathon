import psutil
import os
import logging

# Get the enterprise logger for hardware telemetry
log = logging.getLogger("SystemProfile")

class SystemProfile:
    """
    Enterprise Hardware Intelligence Layer.
    Provides optimized concurrency strategies based on physical constraints.
    """
    
    # 1. Hardware Snapshot (Collected once at startup)
    _LOGICAL_CORES = os.cpu_count() or 2
    _PHYSICAL_CORES = psutil.cpu_count(logical=False) or _LOGICAL_CORES
    _TOTAL_RAM_GB = psutil.virtual_memory().total / (1024 ** 3)
    
    @classmethod
    def get_telemetry(cls):
        """Returns a dictionary of the system DNA."""
        return {
            "logical_cores": cls._LOGICAL_CORES,
            "physical_cores": cls._PHYSICAL_CORES,
            "total_ram_gb": round(cls._TOTAL_RAM_GB, 2),
            "ram_pressure": psutil.virtual_memory().percent
        }

    @classmethod
    def is_legacy_node(cls) -> bool:
        """
        Determines if the machine is 'Legacy' (Old/Slow).
        Criteria: <= 2 Physical Cores OR <= 4GB Total RAM.
        """
        is_slow = cls._PHYSICAL_CORES <= 2 or cls._TOTAL_RAM_GB <= 4.1
        if is_slow:
            log.warning("Hardware Intelligence: LEGACY NODE DETECTED. Throttling for safety.")
        return is_slow

    @classmethod
    def get_optimal_threads(cls, task_type="IO") -> int:
        """
        The Gold Standard formula for thread allocation.
        - IO tasks (Zipping/Uploading): Benefit from more threads (Cores * 2).
        - CPU tasks (Compression): Benefit from physical core alignment (Cores - 1).
        """
        if cls.is_legacy_node():
            return 1

        if task_type == "CPU":
            # Leave 1 core for the OS/Flask to prevent system freeze
            return max(1, cls._PHYSICAL_CORES - 1)
        
        # IO tasks: Max out at 16 to avoid Windows File Handle limits
        return min(16, cls._LOGICAL_CORES * 2)

    @staticmethod
    def has_sufficient_disk(path: str, required_gb: float = 5.0) -> bool:
        """Safety check to ensure the hospital's drive isn't full."""
        usage = psutil.disk_usage(path)
        free_gb = usage.free / (1024 ** 3)
        return free_gb > required_gb