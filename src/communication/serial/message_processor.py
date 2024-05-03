import logging
from abc import ABC, abstractmethod
from src.logger import configure_logging

configure_logging()
logger = logging.getLogger(__name__)

class MessageReader(ABC):
    @abstractmethod
    def read_messages(self, stream):
        pass

class MessageHandler(ABC):
    @abstractmethod
    def process(self, parsed_data, device_id, experiment_id):
        pass

class HandlerRegistry:
    def __init__(self):
        self.handlers = {}

    def _log_handler_action(self, message_type, action, is_warning=False):
        log_message = f"Handler for {message_type} {action} successfully."
        if is_warning:
            logger.warning(log_message)
        else:
            logger.info(log_message)

    def register_handler(self, message_type, handler):
        action = "already registered. Overwriting" if message_type in self.handlers else "registered"
        self.handlers[message_type] = handler
        self._log_handler_action(message_type, action, is_warning=message_type in self.handlers)

    def deregister_handler(self, message_type):
        if self.handlers.pop(message_type, None):
            self._log_handler_action(message_type, "deregistered")
        else:
            self._log_handler_action(message_type, "not registered", is_warning=True)

    def get_handler(self, message_type):
        return self.handlers.get(message_type)

class MessageProcessor:
    def __init__(self, message_reader: MessageReader, handler_registry: HandlerRegistry):
        self.message_reader = message_reader
        self.handler_registry = handler_registry

    def process_data(self, parsed_data, device_id, gnss_messages, experiment_id):
        if parsed_data.identity not in gnss_messages:
            logger.debug(f"Message type not in GNSS messages: {parsed_data.identity}")
            return None

        handler = self.handler_registry.get_handler(parsed_data.identity)
        if not handler:
            logger.warning(f"No handler for message type: {parsed_data.identity}")
            return None

        return handler.process(parsed_data, device_id, experiment_id)