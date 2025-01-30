from ..interfaces.i_notification_service import INotificationService
from typing import Any, TYPE_CHECKING
from ..logging.logger import logger

if TYPE_CHECKING:
    from logging import Logger

class NotificationService(INotificationService):
    def __init__(self) -> None:
        self.logger: "Logger" = logger
    def send_notification(self, message: str, recipient: str) -> None:
        try:
            self.logger.info(f"Sending notification to {recipient}: {message}")
        except Exception as e:
            self.logger.error(f"Failed to send notification to {recipient}: {e}")
            raise
