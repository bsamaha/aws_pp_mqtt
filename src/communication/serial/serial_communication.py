import asyncio
import logging
from typing import Any
from src.communication.handlers.gngga_handler import GNGGAHandler
from src.communication.handlers.navpvt_handler import NAVPVTHandler
from serial import Serial, SerialException
from src.communication.interfaces.communication_interface import CommunicationInterface
from src.communication.serial.message_processor import HandlerRegistry, MessageProcessor
from src.communication.serial.message_reader import MessageReader
from pyubx2 import UBX_PROTOCOL, NMEA_PROTOCOL, RTCM3_PROTOCOL
from src.logger import configure_logging

configure_logging()
logger = logging.getLogger(__name__)

class SerialCommunication(CommunicationInterface):
    def __init__(self, port, baudrate, timeout, device_id, experiment_id, gnss_messages, event_bus):
        self.stream = None
        self.to_receiver_message_queue = asyncio.Queue()
        self.processing_queue = asyncio.Queue()
        self.running = True
        self.device_id = device_id
        self.event_bus = event_bus
        self._open_connection(port, baudrate, timeout)
        self.gnss_messages = gnss_messages
        self.experiment_id = experiment_id
        self.thread = asyncio.create_task(self.send_messages())
        self.receiver_task = asyncio.create_task(self.receive())
        self.processor_task = asyncio.create_task(self.process_messages())
        self.message_reader = MessageReader()
        self.handler_registry = HandlerRegistry()
        self.message_processor = MessageProcessor(self.message_reader, self.handler_registry)
       

        # Subscribe to pp_correction_message event
        self.event_bus.subscribe("pp_correction_message", self.handle_pp_correction_message)
        
        # Ensure the event loop is running and schedule the handler registration
        asyncio.create_task(self.register_message_handlers())


    async def receive(self):
        while self.running:
            try:
                # Use MessageReader instance to read messages from the stream
                for parsed_data in self.message_reader.read_messages(stream=self.stream, protfilter=UBX_PROTOCOL | NMEA_PROTOCOL | RTCM3_PROTOCOL):
                    if parsed_data is not None:
                        await self.processing_queue.put(parsed_data)
                    else:
                        break  # No more data available
            except asyncio.CancelledError:
                logger.info("Receive task was cancelled")
                break
            except Exception as e:
                logger.error(f"Error in receiving data: {e}")
            await asyncio.sleep(0.001)  # Short sleep to yield control


    async def process_messages(self):
        while self.running:
            parsed_data = await self.processing_queue.get()
            logger.debug(f"parsed_data from serial stream: {parsed_data}")
            
            processed_data = self.message_processor.process_data(parsed_data, self.device_id, self.gnss_messages, self.experiment_id)
            if processed_data:
                logger.info("Processed GNSS message: %s", processed_data)
                await self.event_bus.publish(f"gnss_data", processed_data)
            self.processing_queue.task_done()

    async def register_message_handlers(self):
        logger.info("Registering message handlers...")
        self.handler_registry.register_handler("GNGGA", GNGGAHandler())
        self.handler_registry.register_handler("NAV-PVT", NAVPVTHandler())
        logger.info("GNSS message handlers registered.")


    async def connect(self):
        if not self.stream or not self.stream.is_open:
            self._open_connection(self.stream.port, self.stream.baudrate, self.stream.timeout)

    async def disconnect(self):
        self.running = False
        if self.stream and self.stream.is_open:
            self.stream.close()
            logger.info("Serial port closed.")

    def enqueue_message(self, message):
        # Assuming message is a string that needs to be encoded
        self.to_receiver_message_queue.put(message)
        logger.debug(f"Enqueued message: {message}")

    def handle_pp_correction_message(self, data):
        """Process pp_correction_message event from event bus."""
        logger.debug("Enqueuing PP correction message for sending: %s", data)
        # Ensure the message is in the correct format (bytes) before enqueuing
        if isinstance(data, str):
            data = data.encode('utf-8')
        asyncio.create_task(self.to_receiver_message_queue.put(data))

    async def send(self, destination: str, message: Any, **kwargs):
        # For serial, destination is ignored as it's a point-to-point connection.
        if isinstance(message, str):
            message = message.encode('utf-8')  # Convert to bytes
        self.stream.write(message)
        logger.info(f"Sent message to serial: {message}")

    def _open_connection(self, port, baudrate, timeout):
        try:
            self.stream = Serial(port, baudrate, timeout=timeout)
            logger.info("Serial port %s opened successfully.", port)
        except SerialException as e:
            logger.error("Failed to open serial port: %s", e)
            raise ConnectionError(f"Failed to open serial port: {e}") from e

    async def send_messages(self):
        """
        This method is used to send messages from the message queue to the serial port.
        """
        while self.running:
            if not self.to_receiver_message_queue.empty():
                message = await self.to_receiver_message_queue.get()
                try:
                    self.stream.write(message)
                    logger.debug("Serial Message Sent: %s", message)
                except SerialException as e:
                    logger.error("Error sending message: %s", e)
                except asyncio.CancelledError:
                    logger.info("Send messages task was cancelled")
                    break  # Exit the loop if the task is cancelled
            await asyncio.sleep(0.01)


    def close(self):
        self.running = False
        self.thread.cancel()
        self.receiver_task.cancel()
        self.processor_task.cancel()  
        if self.stream:
            self.stream.close()
            logger.info("Serial port closed.")