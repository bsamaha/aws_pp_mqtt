"""
This module is used to connect to the AWS Iot Core broker using the awsiot SDK.
"""

from typing import Any
import logging
from awscrt import io, mqtt
from awsiot import mqtt_connection_builder
import json
import asyncio
from awscrt.mqtt import QoS
from src.logger import configure_logging

configure_logging()
logger = logging.getLogger(__name__)

class IoTCoreClient:
    def __init__(self, endpoint, port, cert_filepath, private_key_filepath, ca_filepath, thing_name, pp_region, event_loop, event_bus):
        logger.debug("Initializing MQTTConnection with endpoint: %s, port: %d", endpoint, port)
        self.endpoint = endpoint
        self.mqtt_port = port
        self.cert_filepath = cert_filepath
        self.private_key_filepath = private_key_filepath
        self.ca_filepath = ca_filepath
        self.thing_name = thing_name
        self.pp_region = pp_region
        self._is_connected = False
        self.event_loop = event_loop
        self.event_bus = event_bus

        self._initialize_connection_components()
        self.mqtt_connection = self._build_connection()
        logger.info("MQTTConnection instance created.")

    def _build_connection(self):
        logger.info("Building MQTT connection with provided credentials.")
        return mqtt_connection_builder.mtls_from_path(
            endpoint=self.endpoint,
            cert_filepath=self.cert_filepath,
            pri_key_filepath=self.private_key_filepath,
            client_bootstrap=self.client_bootstrap,
            ca_filepath=self.ca_filepath,
            client_id=self.thing_name,
            clean_session=False,
            keep_alive_secs=30
        )
    
    def _initialize_connection_components(self):
        self.event_loop_group = io.EventLoopGroup(1)
        self.client_bootstrap = io.ClientBootstrap(self.event_loop_group, io.DefaultHostResolver(self.event_loop_group))

    @property
    def is_connected(self):
        return self._is_connected

    async def connect(self):
        logger.info("Connecting to MQTT broker at %s:%d...", self.endpoint, self.mqtt_port)
        try:
            connect_future = self.mqtt_connection.connect()
            await asyncio.wrap_future(connect_future)
            logger.info("Successfully connected to MQTT broker.")
        except Exception as e:
            logger.error(f"Failed to connect to MQTT broker: {e}")
            raise

    async def disconnect(self):
        logger.info("Disconnecting from MQTT broker at %s:%d...", self.endpoint, self.mqtt_port)
        try:
            disconnect_future = self.mqtt_connection.disconnect()
            await asyncio.wrap_future(disconnect_future)
            logger.info("Successfully disconnected from MQTT broker.")
        except Exception as e:
            logger.error(f"Failed to disconnect from MQTT broker: {e}")
            raise


    async def send(self, destination: str, message: Any, as_json=True, **kwargs):
        if as_json:
            message = json.dumps(message)
        try:
            # Unpack the tuple to get the future and packet_id
            publish_future, packet_id = self.mqtt_connection.publish(
                topic=destination,
                payload=message,
                qos=kwargs.get('qos', mqtt.QoS.AT_LEAST_ONCE)
            )
            # Now, correctly await only the future part of the tuple
            await asyncio.wait_for(asyncio.wrap_future(publish_future), timeout=3)
            logger.debug(f"Message successfully published to {destination}. Packet ID: {packet_id}")
        except asyncio.TimeoutError:
            logger.warning(f"Publish to {destination} timed out.")
        except Exception as e:
            logger.error(f"Error publishing to {destination}: {e}")

    async def receive(self, topic, **kwargs):
        qos = kwargs.get('qos', 0)
        logger.info("Subscribing to topic %s with QoS %s", topic, qos)

        def message_received(topic, payload, **kwargs):
            logger.info(f"Message received on topic {topic}")
            asyncio.run_coroutine_threadsafe(
                self.event_bus.publish("pp_correction_message", payload),
                self.event_loop
            )

        try:
            subscribe_future, _ = self.mqtt_connection.subscribe(
                topic=topic,
                qos=QoS.AT_LEAST_ONCE if qos else QoS.AT_MOST_ONCE,
                callback=message_received
            )
            await asyncio.wrap_future(subscribe_future)
            logger.debug("Subscribe request sent. Waiting for subscription to complete.")
        except asyncio.CancelledError:
            logger.error("Subscription to topic %s was cancelled.", topic)
            raise

    async def unsubscribe(self, topic):
        logger.info("Unsubscribing from topic %s", topic)
        unsubscribe_future = self.mqtt_connection.unsubscribe(topic)
        return await asyncio.wrap_future(unsubscribe_future)