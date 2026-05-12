import queue
import json
import threading

clients = []
clients_lock = threading.Lock()

def add_client(q):
    with clients_lock:
        clients.append(q)

def remove_client(q):
    with clients_lock:
        if q in clients:
            clients.remove(q)

def notify_study_change(study_uid):
    """Pushes a real-time update event to all connected browsers."""
    message = json.dumps({"action": "update_row", "study_uid": study_uid})
    with clients_lock:
        for q in clients:
            try:
                # Instantly drop the message into the client's queue without blocking
                q.put_nowait(message)
            except queue.Full:
                pass