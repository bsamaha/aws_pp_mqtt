from abc import ABC, abstractmethod

class MQTTConnectionInterface(ABC):
    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def subscribe(self, topic, callback):
        pass

    @abstractmethod
    def publish(self, topic, message):
        pass


# from mqtt_interface import MQTTConnectionInterface
from awscrt import io, mqtt
from awsiot import mqtt_connection_builder
import config
import time
import json

class MQTTConnection(MQTTConnectionInterface):
    def __init__(self):
        self.mqtt_connection = self._build_connection()

    def _build_connection(self):
        event_loop_group = io.EventLoopGroup(1)
        host_resolver = io.DefaultHostResolver(event_loop_group)
        client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)
        return mqtt_connection_builder.mtls_from_path(
            endpoint=config.TARGET_EP,
            port=8883,
            cert_filepath=config.CERT_FILEPATH,
            pri_key_filepath=config.PRIVATE_KEY_FILEPATH,
            client_bootstrap=client_bootstrap,
            ca_filepath=config.CA_FILEPATH,
            client_id=config.THING_NAME,
            clean_session=True,
            keep_alive_secs=30)

    def connect(self):
        while True:
            try:
                connect_future = self.mqtt_connection.connect()
                connect_future.result()
            except Exception as e:
                print(f"Connection to IoT Core failed with {e}... retrying in 5s.")
                time.sleep(5)
                continue
            else:
                print("Connected!")
                break

    def subscribe(self, topic, callback):
        subscribe_future, _ = self.mqtt_connection.subscribe(
            topic=topic,
            qos=mqtt.QoS.AT_LEAST_ONCE,
            callback=callback)
        subscribe_result = subscribe_future.result()
        print(f"Subscribed with {str(subscribe_result['qos'])}")

    def publish(self, topic, message, as_json=True, retain=False):
        if as_json:
            message_payload = json.dumps(message)
        else:
            message_payload = message
        self.mqtt_connection.publish(
            topic=topic,
            payload=message_payload,
            qos=mqtt.QoS.AT_LEAST_ONCE,
            retain=retain)