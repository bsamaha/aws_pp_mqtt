from abc import ABC, abstractmethod
import logging
from awscrt import io, mqtt
from awsiot import mqtt_connection_builder
import json
from concurrent.futures import Future

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class MQTTConnectionInterface(ABC):
    @abstractmethod
    def connect(self) -> Future:
        pass

    @abstractmethod
    def subscribe(self, topic, callback) -> Future:
        pass

    @abstractmethod
    def publish(self, topic, message) -> Future:
        pass

class MQTTConnection(MQTTConnectionInterface):
    def __init__(self, endpoint, port, cert_filepath, private_key_filepath, ca_filepath, thing_name):
        logger.debug("Initializing MQTTConnection with endpoint: %s, port: %d", endpoint, port)
        self.endpoint = endpoint
        self.mqtt_port = port
        self.cert_filepath = cert_filepath
        self.private_key_filepath = private_key_filepath
        self.ca_filepath = ca_filepath
        self.thing_name = thing_name
        logger.debug("MQTTConnection parameters set. Building connection...")
        self.mqtt_connection = self._build_connection()
        logger.info("MQTTConnection instance created.")
        # Setup callbacks
        self.mqtt_connection.on_message = self.on_publish_received
        self.mqtt_connection.on_connection_interrupted = self.on_lifecycle_stopped
        self.mqtt_connection.on_connection_resumed = self.on_lifecycle_connection_success


    def _build_connection(self):
        event_loop_group = io.EventLoopGroup(1)
        host_resolver = io.DefaultHostResolver(event_loop_group)
        client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)
        logger.info("Building MQTT connection with provided credentials.")
        connection = mqtt_connection_builder.mtls_from_path(
            endpoint=self.endpoint,
            cert_filepath=self.cert_filepath,
            pri_key_filepath=self.private_key_filepath,
            client_bootstrap=client_bootstrap,
            ca_filepath=self.ca_filepath,
            client_id=self.thing_name,
            clean_session=False,
            keep_alive_secs=30,
            # MQTT 5 specific options
            will_delay_interval_sec=10,
            session_expiry_interval_sec=3600,
            max_packet_size=262144,
            receive_maximum=10
        )
        logger.debug("MQTT connection built successfully.")
        return connection

    def connect(self) -> Future:
        logger.info("Connecting to MQTT broker at %s:%d...", self.endpoint, self.mqtt_port)
        connect_future = self.mqtt_connection.connect()
        logger.debug("Connect request sent. Waiting for connection to establish...")
        return connect_future

    def disconnect(self) -> Future:
        logger.info("Disconnecting from MQTT broker at %s:%d...", self.endpoint, self.mqtt_port)
        disconnect_future = self.mqtt_connection.disconnect()
        logger.debug("Disconnect request sent. Waiting for disconnection...")
        return disconnect_future

    def subscribe(self, topic, callback, qos=0) -> Future:
        logger.info(f"Subscribing to topic {topic} with QoS {qos}...")
        subscribe_future, packet_id = self.mqtt_connection.subscribe(
            topic=topic,
            qos=mqtt.QoS.AT_LEAST_ONCE,
            callback=callback)
        logger.debug("Subscribe request sent. Waiting for subscription to complete...")
        return subscribe_future
    
    def unsubscribe(self, topic) -> Future:
        """
        Unsubscribes from a topic.

        :param topic: The MQTT topic to unsubscribe from.
        :return: A Future representing the unsubscription result.
        """
        logger.info(f"Unsubscribing from topic {topic}...")
        unsubscribe_future = self.mqtt_connection.unsubscribe(topic)
        logger.debug("Unsubscribe request sent. Waiting for unsubscription to complete...")
        return unsubscribe_future

    def publish(self, topic, message, as_json=True, retain=False, properties=None) -> Future:
        logger.debug(f"Preparing to publish message to {topic}. JSON: {as_json}, Retain: {retain}")
        if as_json:
            message_payload = json.dumps(message)
            logger.debug("Message converted to JSON.")
        else:
            message_payload = message
        logger.info(f"Publishing message to {topic}...")
        publish_future = self.mqtt_connection.publish(
            topic=topic,
            payload=message_payload,
            qos=mqtt.QoS.AT_LEAST_ONCE,
            retain=retain,
            properties=properties)  # Utilizing MQTT 5 properties
        logger.debug("Publish request sent. Waiting for message to be published...")
        logger.info("Published message to %s", topic)
        return publish_future
    
    def on_publish_received(self, topic, payload, **kwargs):
        logger.debug("Received message from topic '%s': %s", topic, payload)

    def on_lifecycle_stopped(self, lifecycle_stopped_data):
        logger.info("Lifecycle Stopped")

    def on_lifecycle_connection_success(self, lifecycle_connect_success_data):
        logger.info("Lifecycle Connection Success")

    def on_lifecycle_connection_failure(self, lifecycle_connection_failure):
        logger.error("Lifecycle Connection Failure")
        logger.error("Connection failed with exception: %s", lifecycle_connection_failure.exception)
