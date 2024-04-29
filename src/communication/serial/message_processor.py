import logging
from serial import SerialException
from pyubx2 import UBX_PROTOCOL, NMEA_PROTOCOL, RTCM3_PROTOCOL
from src.communication.serial.message_reader import MessageReader
from src.logger import configure_logging

configure_logging()
logger = logging.getLogger(__name__)

class MessageProcessor:
    """
    Processes GNSS messages using dynamically registered handlers.
    """
    handlers = {}

    @classmethod
    def register_handler(cls, message_type, handler):
        """
        Registers a handler for a specific message type.

        :param message_type: The type of the message the handler can process.
        :param handler: The handler instance.
        """
        if message_type in cls.handlers:
            logger.warning(f"Handler for {message_type} already registered. Overwriting.")
        cls.handlers[message_type] = handler
        logger.info(f"Handler for {message_type} registered successfully.")

    @classmethod
    def deregister_handler(cls, message_type):
        """
        Deregisters the handler for a specific message type.

        :param message_type: The type of the message to deregister the handler for.
        """
        if message_type in cls.handlers:
            del cls.handlers[message_type]
            logger.info(f"Handler for {message_type} deregistered successfully.")
        else:
            logger.warning(f"No handler registered for {message_type}.")

    @classmethod
    def process_data(cls, stream, device_id, gnss_messages, experiment_id):
        message_reader = MessageReader()
        try:
            for parsed_data in message_reader.read_messages(stream, UBX_PROTOCOL | NMEA_PROTOCOL | RTCM3_PROTOCOL):
                if parsed_data.identity not in gnss_messages:
                    logger.debug(f"Message type not in GNSS messages: {parsed_data.identity}")
                    continue

                handler = cls.handlers.get(parsed_data.identity)
                if not handler:
                    logger.warning(f"No handler for message type: {parsed_data.identity}")
                    continue

                return handler.process(parsed_data, device_id, experiment_id)
        except SerialException as e:
            logger.error(f"Error reading data from serial port: {e}")
        except Exception as e:
            logger.error(f"Unexpected error in MessageProcessor: {e}")
        finally:
            if stream.in_waiting:
                logger.debug("Data still available in stream after processing.")
            else:
                logger.debug("No more data available in stream.")
        return None