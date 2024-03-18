import logging
from config.s2s_config import load_config
from aws_mqtt import MQTTConnection
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

if __name__ == '__main__':
    config = load_config()
    logger.debug(config)

    # Setup MQTT Connection
    mqtt_connection = MQTTConnection(
        endpoint=config.target_ep,
        port=config.mqtt_port,
        cert_filepath=config.cert_filepath,
        private_key_filepath=config.private_key_filepath,
        ca_filepath=config.ca_filepath,
        thing_name=config.thing_name
    )

    logger.info("Connecting to %s with client ID '%s'...", config.target_ep, config.thing_name)
    mqtt_connection.connect().result()  # Wait for connection
    logger.info("Connected!")

    message_topic = "/pp/Lb/us/+"

    subscribe_future = mqtt_connection.subscribe(message_topic, mqtt_connection.on_publish_received, qos=0)
    subscribe_future.result()  # Wait for subscription to complete
    logger.info("Subscribed to topic '%s' with QoS 0", message_topic)
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, exiting...")
    finally:
        # Unsubscribing from a topic
        logger.info("Unsubscribing from topic '%s'", message_topic)
        unsubscribe_future = mqtt_connection.unsubscribe(message_topic)
        unsubscribe_future.result()  # Wait for unsubscription
        logger.info("Unsubscribed from topic '%s'", message_topic)

        logger.info("Disconnecting...")
        mqtt_connection.disconnect().result()  # Wait for disconnection
        logger.info("Disconnected!")