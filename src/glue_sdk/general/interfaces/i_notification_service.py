from abc import ABC, abstractmethod

class INotificationService(ABC):

    @abstractmethod
    def send_notification(self, message: str, recipient: str) -> None:
        """Sends a notification to the specified recipient"""
        pass
