import logging
from aws_mqtt import MQTTConnection
from pp_mqtt import PointPerfectClient
from config import load_config

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def on_pp_message_received(client, userdata, message):
    """
    Callback function to handle messages received from PointPerfect MQTT broker.
    Forwards the received message to AWS IoT Core using the same topic.
    """
    logger.info(f"Received message from PointPerfect: {message.topic} - Forwarding to AWS IoT Core on the same topic")
    # Forward the message to AWS IoT Core using the same topic
    aws_mqtt_connection.publish(message.topic, message.payload, as_json=False, retain=True)

if __name__ == "__main__":
    # Load configuration
    config = load_config()

    # Initialize AWS MQTT Connection
    aws_mqtt_connection = MQTTConnection()
    aws_mqtt_connection.connect()

    # Initialize PointPerfect MQTT Client
    pp_client = PointPerfectClient(config, serial_communication=None,s2s=True)  # Assuming no serial communication is needed for this task
    pp_client.mqtt_client.on_message = on_pp_message_received  # Set the callback for handling messages

    # Connect to PointPerfect MQTT Broker
    pp_client.connect()

    try:
        # Keep the script running to maintain connections
        while True:
            pass
    except KeyboardInterrupt:
        # Disconnect gracefully on keyboard interrupt
        pp_client.disconnect()
        aws_mqtt_connection.disconnect()  # Ensure AWS MQTT connection is also gracefully disconnected
        logger.info("Disconnected from PointPerfect MQTT Broker and AWS IoT Core.")