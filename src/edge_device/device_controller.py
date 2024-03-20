import asyncio
import logging
from src.mqtt.aws_mqtt import MQTTConnection
from src.serial.serial_communication import SerialCommunication

logger = logging.getLogger(__name__)

class DeviceController:
    def __init__(self, config):
        self.config = config
        self.mqtt_connection = MQTTConnection(
            endpoint=config.target_ep,
            port=config.mqtt_port,
            cert_filepath=config.cert_filepath,
            private_key_filepath=config.private_key_filepath,
            ca_filepath=config.ca_filepath,
            thing_name=config.thing_name,
            pp_region=config.pp_region
        )
        self.serial_comm = SerialCommunication(
            port=config.serial_port,
            baudrate=config.baudrate,
            timeout=1,
            device_id=config.alias,
            experiment_id=config.experiment_id,
            gnss_messages=config.gnss_messages,
            mqtt_client=self.mqtt_connection
        )

    async def start(self):
        logger.info("Connecting to MQTT and setting up Serial...")
        await self.mqtt_connection.connect()
        await self.serial_comm.register_message_handlers()  # Register message handlers
        asyncio.create_task(self.serial_comm.read_and_send_data())
        # Use the receive method instead of directly calling subscribe
        logger.info(f"Subscribing to PP region {self.config.pp_region}")
        await self.mqtt_connection.receive("/pp/ubx/0236/Lb", self.on_message_received, qos=1)
        await self.mqtt_connection.receive("/pp/ubx/mga", self.on_message_received, qos=1)
        await self.mqtt_connection.receive(f"/pp/Lb/{self.pp_region}/+", self.on_message_received, qos=0)

    def on_message_received(self, topic, payload, **kwargs):
        logger.debug(f"Message received on topic {topic}: {payload}")
        # Ensure payload is processed correctly, potentially parsing JSON or other formats
        self.serial_comm.enqueue_message(payload)

    async def stop(self):
        logger.info("Cleaning up connections...")
        await self.mqtt_connection.unsubscribe("/pp/Lb/us/+")
        await self.mqtt_connection.disconnect()
        self.serial_comm.close()