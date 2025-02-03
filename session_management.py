import time
import random
import threading
from abc import ABC, abstractmethod
from typing import Dict, Optional
import uuid
from queue import Queue

# Session Data
class Session:
    def __init__(self, user_id: str, session_id: str, expires_at: float):
        self.user_id = user_id
        self.session_id = session_id
        self.expires_at = expires_at

# Session Storage Interface
class SessionStorage(ABC):
    @abstractmethod
    def create_session(self, user_id: str, expires_at: float) -> Session:
        pass

    @abstractmethod
    def get_session(self, session_id: str) -> Optional[Session]:
        pass

    @abstractmethod
    def delete_session(self, session_id: str) -> bool:
        pass

    @abstractmethod
    def update_session_expiry(self, session_id: str, expires_at: float) -> bool:
        pass

# Redis Session Storage (Simulated)
class RedisSessionStorage(SessionStorage):
    def __init__(self):
        self.sessions: Dict[str, Session] = {}
        self.lock = threading.Lock()

    def create_session(self, user_id: str, expires_at: float) -> Session:
        with self.lock:
            session_id = str(uuid.uuid4())
            session = Session(user_id, session_id, expires_at)
            self.sessions[session_id] = session
            return session

    def get_session(self, session_id: str) -> Optional[Session]:
        with self.lock:
            return self.sessions.get(session_id)

    def delete_session(self, session_id: str) -> bool:
        with self.lock:
            if session_id in self.sessions:
                del self.sessions[session_id]
                return True
            return False

    def update_session_expiry(self, session_id: str, expires_at: float) -> bool:
        with self.lock:
            if session_id in self.sessions:
                self.sessions[session_id].expires_at = expires_at
                return True
            return False

# Database Session Storage (Simulated)
class DatabaseSessionStorage(SessionStorage):
    def __init__(self):
        self.sessions: Dict[str, Session] = {}
        self.lock = threading.Lock()

    def create_session(self, user_id: str, expires_at: float) -> Session:
        with self.lock:
            session_id = str(uuid.uuid4())
            session = Session(user_id, session_id, expires_at)
            self.sessions[session_id] = session
            return session

    def get_session(self, session_id: str) -> Optional[Session]:
        with self.lock:
            return self.sessions.get(session_id)

    def delete_session(self, session_id: str) -> bool:
        with self.lock:
            if session_id in self.sessions:
                del self.sessions[session_id]
                return True
            return False

    def update_session_expiry(self, session_id: str, expires_at: float) -> bool:
        with self.lock:
            if session_id in self.sessions:
                self.sessions[session_id].expires_at = expires_at
                return True
            return False

# Session Manager
class SessionManager:
    def __init__(self, session_storage: SessionStorage, session_timeout: int = 3600):
        self.session_storage = session_storage
        self.session_timeout = session_timeout
        self.running = True

    def start(self):
        def cleanup_expired_sessions():
            while self.running:
                current_time = time.time()
                sessions_to_delete = []
                for session_id, session in self.session_storage.sessions.items():
                    if session.expires_at <= current_time:
                        sessions_to_delete.append(session_id)
                for session_id in sessions_to_delete:
                    self.session_storage.delete_session(session_id)
                time.sleep(60)  # Cleanup every 60 seconds

        threading.Thread(target=cleanup_expired_sessions, daemon=True).start()

    def stop(self):
        self.running = False

    def create_session(self, user_id: str) -> Session:
        expires_at = time.time() + self.session_timeout
        return self.session_storage.create_session(user_id, expires_at)

    def get_session(self, session_id: str) -> Optional[Session]:
        return self.session_storage.get_session(session_id)

    def delete_session(self, session_id: str) -> bool:
        return self.session_storage.delete_session(session_id)

    def update_session_expiry(self, session_id: str) -> bool:
        expires_at = time.time() + self.session_timeout
        return self.session_storage.update_session_expiry(session_id, expires_at)

# Simulate User Activity
def simulate_user_activity(session_manager: SessionManager):
    user_ids = [f"user_{i}" for i in range(10)]
    while True:
        user_id = random.choice(user_ids)
        session = session_manager.create_session(user_id)
        print(f"Session created for User {user_id}: {session.session_id}")
        time.sleep(1)  # Simulate activity every second

# Main Function
def main():
    # Create session storage (Redis or Database)
    session_storage = RedisSessionStorage()  # or DatabaseSessionStorage()
    session_manager = SessionManager(session_storage)
    session_manager.start()

    # Simulate user activity in a separate thread
    threading.Thread(target=simulate_user_activity, args=(session_manager,), daemon=True).start()

    # Keep the main thread alive to allow background processing
    time.sleep(20)
    session_manager.stop()

if __name__ == "__main__":
    main()