from ...core.logging.logger import logger
from ...core.interfaces.i_base_service import IBaseService
from ...core.services.notification_service import NotificationService
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from logging import Logger    

class BaseService(NotificationService,IBaseService):
    def __init__(self) -> None:
        super().__init__()
        self.logger: "Logger" = logger

    def log_info(self, message: str) -> None:
        self.logger.info(msg=message)

    def log_error(self, message: str) -> None:
        self.logger.error(msg=message)

    def log_warning(self, message: str) -> None:
        self.logger.warning(msg=message)
    
    def log_debug(self, message: str) -> None:
        self.logger.debug(msg=message)
