import queue
import json
import threading
import time

clients = []
clients_lock = threading.Lock()

# Event Aggregator variables
pending_studies = set()
aggregator_lock = threading.Lock()

def add_client(q):
    with clients_lock:
        clients.append(q)

def remove_client(q):
    with clients_lock:
        if q in clients:
            clients.remove(q)

def notify_study_change(study_uid):
    """Adds the study_uid to the aggregator batch."""
    with aggregator_lock:
        pending_studies.add(study_uid)

def _aggregator_loop():
    """Flushes aggregated study updates every 1 second."""
    while True:
        time.sleep(1.0)
        
        with aggregator_lock:
            if not pending_studies:
                continue
            # Swap out the current batch
            batch = list(pending_studies)
            pending_studies.clear()
            
        # Send one message per unique study_uid that changed in the last second
        with clients_lock:
            for study_uid in batch:
                message = json.dumps({"action": "update_row", "study_uid": study_uid})
                for q in clients:
                    try:
                        q.put_nowait(message)
                    except queue.Full:
                        pass

# Start the aggregator daemon thread
threading.Thread(target=_aggregator_loop, daemon=True, name="SSE_Aggregator").start()
