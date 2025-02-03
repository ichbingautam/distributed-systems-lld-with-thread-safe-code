import time
import threading
from abc import ABC, abstractmethod
from typing import Dict, Optional, List
import random
from queue import Queue

# Feature Toggle
class FeatureToggle:
    def __init__(self, toggle_id: str, description: str, enabled: bool = False, rollout_percentage: float = 0.0, ab_test_group: Optional[str] = None):
        self.toggle_id = toggle_id
        self.description = description
        self.enabled = enabled
        self.rollout_percentage = rollout_percentage  # Percentage of users to enable the feature for
        self.ab_test_group = ab_test_group  # A/B testing group (A or B)

    def is_enabled_for_user(self, user_id: str) -> bool:
        if not self.enabled:
            return False
        # Simulate percentage-based rollout
        if self.rollout_percentage > 0:
            return hash(user_id) % 100 < self.rollout_percentage * 100
        # Simulate A/B testing
        if self.ab_test_group:
            return (hash(user_id) % 2 == 0 and self.ab_test_group == "A") or (hash(user_id) % 2 == 1 and self.ab_test_group == "B")
        return False

# Feature Toggle Storage Interface
class FeatureToggleStorage(ABC):
    @abstractmethod
    def get_toggle(self, toggle_id: str) -> Optional[FeatureToggle]:
        pass

    @abstractmethod
    def update_toggle(self, toggle: FeatureToggle) -> bool:
        pass

    @abstractmethod
    def get_all_toggles(self) -> List[FeatureToggle]:
        pass

# In-Memory Feature Toggle Storage (Simulated)
class InMemoryFeatureToggleStorage(FeatureToggleStorage):
    def __init__(self):
        self.toggles: Dict[str, FeatureToggle] = {}
        self.lock = threading.Lock()

    def get_toggle(self, toggle_id: str) -> Optional[FeatureToggle]:
        with self.lock:
            return self.toggles.get(toggle_id)

    def update_toggle(self, toggle: FeatureToggle) -> bool:
        with self.lock:
            self.toggles[toggle.toggle_id] = toggle
            return True

    def get_all_toggles(self) -> List[FeatureToggle]:
        with self.lock:
            return list(self.toggles.values())

# Feature Toggle Manager
class FeatureToggleManager:
    def __init__(self, storage: FeatureToggleStorage):
        self.storage = storage

    def is_feature_enabled(self, toggle_id: str, user_id: str) -> bool:
        toggle = self.storage.get_toggle(toggle_id)
        if toggle:
            return toggle.is_enabled_for_user(user_id)
        return False

    def update_toggle(self, toggle: FeatureToggle) -> bool:
        return self.storage.update_toggle(toggle)

    def get_all_toggles(self) -> List[FeatureToggle]:
        return self.storage.get_all_toggles()

# Simulate Feature Toggle Updates
def simulate_toggle_updates(feature_toggle_manager: FeatureToggleManager):
    toggles = [
        FeatureToggle("surge_pricing", "Enable surge pricing", enabled=True, rollout_percentage=0.5),
        FeatureToggle("new_ui", "Enable new UI", enabled=False, ab_test_group="A"),  # A/B testing for new UI
    ]
    for toggle in toggles:
        feature_toggle_manager.update_toggle(toggle)

    while True:
        # Simulate dynamic toggle updates
        time.sleep(10)
        new_toggle = FeatureToggle("new_ui", "Enable new UI", enabled=True, ab_test_group="B")  # Switch to group B
        feature_toggle_manager.update_toggle(new_toggle)
        print("Updated new_ui toggle to group B for A/B testing.")

# Simulate User Activity
def simulate_user_activity(feature_toggle_manager: FeatureToggleManager):
    user_ids = [f"user_{i}" for i in range(100)]
    while True:
        user_id = random.choice(user_ids)
        if feature_toggle_manager.is_feature_enabled("surge_pricing", user_id):
            print(f"Surge pricing enabled for User {user_id}.")
        if feature_toggle_manager.is_feature_enabled("new_ui", user_id):
            print(f"New UI enabled for User {user_id} (A/B testing).")
        time.sleep(1)  # Simulate activity every second

# Main Function
def main():
    # Create feature toggle storage and manager
    storage = InMemoryFeatureToggleStorage()
    feature_toggle_manager = FeatureToggleManager(storage)

    # Simulate toggle updates in a separate thread
    threading.Thread(target=simulate_toggle_updates, args=(feature_toggle_manager,), daemon=True).start()

    # Simulate user activity in a separate thread
    threading.Thread(target=simulate_user_activity, args=(feature_toggle_manager,), daemon=True).start()

    # Keep the main thread alive to allow background processing
    time.sleep(60)

if __name__ == "__main__":
    main()