from typing import Any, Callable
from src.interfaces.communication_interface import CommunicationInterface
import logging
from awscrt import io, mqtt
from awsiot import mqtt_connection_builder
import json
import asyncio
from awscrt.mqtt import QoS  # Make sure to import QoS


logger = logging.getLogger(__name__)

class MQTTConnection(CommunicationInterface):
    def __init__(self, endpoint, port, cert_filepath, private_key_filepath, ca_filepath, thing_name, event_loop_group=None, client_bootstrap=None):
        logger.debug("Initializing MQTTConnection with endpoint: %s, port: %d", endpoint, port)
        self.endpoint = endpoint
        self.mqtt_port = port
        self.cert_filepath = cert_filepath
        self.private_key_filepath = private_key_filepath
        self.ca_filepath = ca_filepath
        self.thing_name = thing_name

        self.event_loop_group = event_loop_group if event_loop_group else io.EventLoopGroup(1)
        self.client_bootstrap = client_bootstrap if client_bootstrap else io.ClientBootstrap(self.event_loop_group, io.DefaultHostResolver(self.event_loop_group))

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
            keep_alive_secs=30,
            # MQTT v5 features and connection lifecycle callbacks
            max_reconnect_delay_ms=128000,
            min_connected_time_to_reset_reconnect_delay_ms=60000,
            ping_timeout_ms=5000,
            connack_timeout_ms=2000,
            ack_timeout_sec=5,
            on_connection_interrupted=self.on_connection_interrupted,
            on_connection_resumed=self.on_connection_resumed,
            # Additional MQTT v5 settings can be configured here as needed
        )

    def on_connection_interrupted(self, connection, error, **kwargs):
        logger.warning(f"Connection interrupted. Error: {error}")

    def on_connection_resumed(self, connection, return_code, session_present, **kwargs):
        logger.info(f"Connection resumed. Return code: {return_code}, Session present: {session_present}")

    async def connect(self):
        logger.info("Connecting to MQTT broker at %s:%d...", self.endpoint, self.mqtt_port)
        connect_future = self.mqtt_connection.connect()
        await asyncio.wrap_future(connect_future)  # Wrap and await the future

    async def disconnect(self):
        logger.info("Disconnecting from MQTT broker at %s:%d...", self.endpoint, self.mqtt_port)
        disconnect_future = self.mqtt_connection.disconnect()
        await asyncio.wrap_future(disconnect_future)  # Wrap and await the future


    async def send(self, destination: str, message: Any, as_json=True, **kwargs):
        if as_json:
            message = json.dumps(message)
        publish_future, _ = self.mqtt_connection.publish(
            topic=destination,
            payload=message,
            qos=kwargs.get('qos', mqtt.QoS.AT_MOST_ONCE)
        )
        try:
            # Wait for the future with a timeout (e.g., 5 seconds)
            await asyncio.wait_for(asyncio.wrap_future(publish_future), timeout=3)
        except asyncio.TimeoutError:
            # Handle timeout (e.g., log a warning)
            logger.warning(f"Publish to {destination} timed out.")
        except Exception as e:
            # Handle other exceptions
            logger.error(f"Error publishing to {destination}: {e}")

    async def subscribe(self, topic, callback, qos=0):
        logger.info(f"Subscribing to topic {topic} with QoS {qos}...")
        subscribe_future, _ = self.mqtt_connection.subscribe(
            topic=topic,
            qos=QoS.AT_LEAST_ONCE if qos else QoS.AT_MOST_ONCE,
            callback=callback)
        await asyncio.wrap_future(subscribe_future)  # Wrap and await the future
        logger.debug("Subscribe request sent. Waiting for subscription to complete...")

    async def unsubscribe(self, topic):
        logger.info(f"Unsubscribing from topic {topic}...")
        unsubscribe_future = self.mqtt_connection.unsubscribe(topic)
        await asyncio.wrap_future(unsubscribe_future)  # Wrap and await the future

    async def receive(self, topic, callback, **kwargs):
        """
        Subscribes to a given topic and sets a callback function to handle incoming messages.

        :param topic: The MQTT topic to subscribe to.
        :param callback: The callback function that is called when a message is received on the subscribed topic.
        :param kwargs: Additional keyword arguments. Can include 'qos' for Quality of Service.
        """
        # Correctly pass the QoS value as a named argument
        qos = kwargs.get('qos', 0)
        await self.subscribe(topic, callback, qos=qos)

    def on_publish_received(self, topic, payload, **kwargs):
        logger.debug("Received message from topic '%s': %s", topic, payload)

    def on_lifecycle_stopped(self, lifecycle_stopped_data):
        logger.info("Lifecycle Stopped")

    def on_lifecycle_connection_success(self, lifecycle_connect_success_data):
        logger.info("Lifecycle Connection Success")

    def on_lifecycle_connection_failure(self, lifecycle_connection_failure):
        logger.error("Lifecycle Connection Failure")
        logger.error("Connection failed with exception: %s", lifecycle_connection_failure.exception)
