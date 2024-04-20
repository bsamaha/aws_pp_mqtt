# Import necessary modules at the top
from abc import ABC, abstractmethod
from datetime import datetime, timezone
import time
from typing import Any, Callable
from pyubx2 import UBXReader, NMEA_PROTOCOL, RTCM3_PROTOCOL, UBX_PROTOCOL
from serial import SerialException
import logging
import os
import asyncio
import queue
from serial import Serial
from src.interfaces.communication_interface import CommunicationInterface

logger = logging.getLogger(__name__)

# Define the base handler interface
class MessageHandler(ABC):
    @abstractmethod
    def process(self, parsed_data, device_id, experiment_id):
        pass

# Implement a handler for GNGGA messages
class GNGGAHandler(MessageHandler):
    def process(self, parsed_data, device_id, experiment_id):

        data_dict = {
            "message_type": parsed_data.identity,
            "full_time": datetime.now(tz=timezone.utc).isoformat(),  
            "lat": parsed_data.lat,
            "ns": parsed_data.NS,
            "lon": parsed_data.lon,
            "ew": parsed_data.EW,
            "quality": parsed_data.quality,
            "num_sv": parsed_data.numSV,
            "hdop": parsed_data.HDOP,
            "alt": parsed_data.alt,
            "alt_unit": parsed_data.altUnit,
            "sep": parsed_data.sep,
            "sep_unit": parsed_data.sepUnit,
            "diff_age": parsed_data.diffAge or None,
            "diff_station": parsed_data.diffStation,
            "processed_time": f"{int(time.time()*1000)}",
            "device_id": device_id,
            "experiment_id": experiment_id
        }
        return data_dict
    

class NAVPVTHandler(MessageHandler):
    def process(self, parsed_data, device_id, experiment_id):
        logger.info("Processing NAV-PVT message: %s", parsed_data)

        full_time = datetime.now(tz=timezone.utc).isoformat() 
        logger.debug(parsed_data)
        data_dict = {
            "message_type": "NAV-PVT",
            "full_time": full_time,
            "year": parsed_data.year,
            "month": parsed_data.month,
            "day": parsed_data.day,
            "hour": parsed_data.hour,
            "min": parsed_data.min,
            "second": parsed_data.second,
            "validDate": parsed_data.validDate,
            "validTime": parsed_data.validTime,
            "tAcc": parsed_data.tAcc,
            "fixType": parsed_data.fixType,
            "gnssFixOk": parsed_data.gnssFixOk,
            "numSV": parsed_data.numSV,
            "lon": parsed_data.lon,
            "lat": parsed_data.lat,
            "height": parsed_data.height,
            "hMSL": parsed_data.hMSL,
            "hAcc": parsed_data.hAcc,
            "vAcc": parsed_data.vAcc,
            "velN": parsed_data.velN,
            "velE": parsed_data.velE,
            "velD": parsed_data.velD,
            "gSpeed": parsed_data.gSpeed,
            "headMot": parsed_data.headMot,
            "sAcc": parsed_data.sAcc,
            "headAcc": parsed_data.headAcc,
            "pDOP": parsed_data.pDOP,
            "device_id": device_id,
            "experiment_id": experiment_id
        }
        return data_dict

class MessageReader:
    """
    Responsible for reading messages from a stream.
    """
    @staticmethod
    def read_messages(stream, protfilter):
        ubx_reader = UBXReader(stream, protfilter=protfilter)
        while stream.in_waiting:
            try:
                _, parsed_data = ubx_reader.read()
                if parsed_data is None:
                    break  # No more data available
                yield parsed_data
            except Exception as e:
                logger.error("Error reading GNSS message: %s", e)
                continue 

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

        # Subscribe to pp_correction_message event
        self.event_bus.subscribe("pp_correction_message", self.handle_pp_correction_message)
        
        # Ensure the event loop is running and schedule the handler registration
        asyncio.create_task(self.register_message_handlers())


    async def receive(self):
        while self.running:
            try:
                # Use MessageReader to read messages from the stream
                for parsed_data in MessageReader.read_messages(self.stream, UBX_PROTOCOL | NMEA_PROTOCOL | RTCM3_PROTOCOL):
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
            parsed_data = await self.processing_queue.get()  # Wait for and get data from the processing queue
            logger.debug(f"parsed_data from serial stream: {parsed_data}")
            if parsed_data.identity not in self.gnss_messages:
                logger.warning(f"Message type not in GNSS messages: {parsed_data.identity}")
                self.processing_queue.task_done()
                continue

            handler = MessageProcessor.handlers.get(parsed_data.identity)
            if not handler:
                logger.warning(f"No handler for message type: {parsed_data.identity}, available handlers are {MessageProcessor.handlers.keys()}, the handlers are supposed to be for {self.gnss_messages}")
                self.processing_queue.task_done()
                continue

            processed_data = handler.process(parsed_data, self.device_id, self.experiment_id)
            if processed_data:
                logger.info("Processed GNSS message: %s", processed_data)
                # Publish the processed data to the event bus
                await self.event_bus.publish(f"gnss_data", processed_data)
            self.processing_queue.task_done()


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
        logger.info(f"Enqueued message: {message}")

    def handle_pp_correction_message(self, data):
        """Process pp_correction_message event from event bus."""
        logger.info("Enqueuing PP correction message for sending: %s", data)
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

    async def register_message_handlers(self):
        # Ensure this method is called during initialization or before processing starts
        logger.info("Registering message handlers...")
        MessageProcessor.register_handler("GNGGA", GNGGAHandler())
        MessageProcessor.register_handler("NAV-PVT", NAVPVTHandler())
        logger.info("GNSS message handlers registered.")

    def close(self):
        self.running = False
        self.thread.cancel()
        self.receiver_task.cancel()
        self.processor_task.cancel()  
        if self.stream:
            self.stream.close()
            logger.info("Serial port closed.")