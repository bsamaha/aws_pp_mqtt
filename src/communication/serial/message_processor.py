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

    def register_handler(self, message_type, handler):
        if message_type in self.handlers:
            logger.warning(f"Handler for {message_type} already registered. Overwriting.")
        self.handlers[message_type] = handler
        logger.info(f"Handler for {message_type} registered successfully.")

    def deregister_handler(self, message_type):
        if message_type in self.handlers:
            del self.handlers[message_type]
            logger.info(f"Handler for {message_type} deregistered successfully.")
        else:
            logger.warning(f"No handler registered for {message_type}.")

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