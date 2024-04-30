from abc import ABC, abstractmethod
import asyncio
import json
import logging
from typing import Any, Callable
from src.logger import configure_logging

configure_logging()
logger = logging.getLogger(__name__)

class CommunicationInterface(ABC):
    """
    Defines a generic interface for communication mechanisms.
    This interface adheres to the SOLID principles, particularly to the
    Single Responsibility Principle by defining a clear and concise contract
    for communication actions.
    """

    @abstractmethod
    async def connect(self):
        """
        Establishes a connection with the communication medium.
        """
        pass

    @abstractmethod
    async def disconnect(self):
        """
        Closes the connection with the communication medium.
        """
        pass

    @abstractmethod
    async def send(self, destination: str, message: Any, **kwargs):
        """
        Sends a message to the specified destination.
        """
        pass

    @abstractmethod
    async def receive(self, source: str, callback: Callable[[str, Any], None], **kwargs):
        """
        Registers a callback to receive messages from the specified source.
        """
        pass

class MQTTCommunication(CommunicationInterface):
    """
    Implements the CommunicationInterface for MQTT communication.
    """

    def __init__(self, mqtt_client):
        self.mqtt_client = mqtt_client

    async def connect(self):
        await self.mqtt_client.connect()

    async def disconnect(self):
        await self.mqtt_client.disconnect()

    async def send(self, destination: str, message: Any, as_json=True, **kwargs):
        if as_json:
            message = json.dumps(message)
        await self.mqtt_client.publish(destination, message, **kwargs)

    async def receive(self, source: str, callback: Callable[[str, Any], None], **kwargs):
        await self.mqtt_client.subscribe(source, callback, **kwargs)

class SerialCommunication(CommunicationInterface):
    """
    Implements the CommunicationInterface for Serial communication.
    """

    def __init__(self, serial_port):
        self.serial_port = serial_port

    async def connect(self):
        if not self.serial_port.is_open:
            self.serial_port.open()

    async def disconnect(self):
        if self.serial_port.is_open:
            self.serial_port.close()

    async def send(self, destination: str, message: Any, **kwargs):
        if isinstance(message, str):
            message = message.encode('utf-8')
        self.serial_port.write(message)

    async def receive(self, source: str, callback: Callable[[str, Any], None], **kwargs):
        # For serial, source is ignored. This method sets up a listener loop.
        while True:
            if self.serial_port.in_waiting > 0:
                data = self.serial_port.readline()
                asyncio.create_task(callback(source, data))
            await asyncio.sleep(0.01)