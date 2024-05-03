# device_controller.py
import asyncio
import logging
from src.communication.interfaces.communication_interface import CommunicationInterface
from src.edge_device.event_bus import EventBus
from src.logger import configure_logging

configure_logging()
logger = logging.getLogger(__name__)

class DeviceController:
    def __init__(self, config, mqtt_comm: CommunicationInterface, serial_comm: CommunicationInterface, event_bus: EventBus):
        self.config = config
        self.event_bus = event_bus
        self.mqtt_comm = mqtt_comm
        self.serial_comm = serial_comm

        # Subscribe to events
        self.setup_subscriptions()

    def setup_subscriptions(self):
        self.event_bus.subscribe("gnss_data", self.on_gnss_data_received)
        self.event_bus.subscribe("mqtt_data_received", self.on_mqtt_data_received)
        self.event_bus.subscribe("pp_correction_message", self.serial_comm.handle_pp_correction_message)
        self.event_bus.subscribe("batch_data_raw_gnss_measurements", self.on_batch_data_raw_gnss_measurements)

    async def start(self):
        logger.info("Connecting to MQTT and setting up Serial...")
        await self.connect_services()
        await self.subscribe_to_topics()

    async def connect_services(self):
        try:
            await self.mqtt_comm.connect()
            await self.serial_comm.connect()
        except Exception as e:
            logger.error(f"Failed to connect services: {e}")
            await self.cleanup_on_error()
            raise

    async def subscribe_to_topics(self):
        topics = [
            ("/pp/ubx/0236/Lb", 1),
            ("/pp/ubx/mga", 1),
            (f"/pp/Lb/{self.config.pp_region}/+", 0)
        ]
        for topic, qos in topics:
            try:
                await self.mqtt_comm.receive(topic=topic, callback=self.on_mqtt_data_received, qos=qos)
            except Exception as e:
                logger.error(f"Failed to subscribe to topic {topic}: {e}")
                await self.cleanup_on_error()
                raise

    async def cleanup_on_error(self):
        await self.mqtt_comm.disconnect()
        await self.serial_comm.disconnect()

    async def on_gnss_data_received(self, data):
        message_type = data.get('message_type').lower().replace("_", "").replace("-", "")
        if message_type:
            rule = f"$aws/rules/{message_type}_data_basic_ingest/{self.config.ts_domain_name}/{self.config.device_id}/{message_type}"
            logger.info(f"sending gnss data to {rule}")
            await self.mqtt_comm.send(rule, data)
        else:
            logger.warning("Received GNSS data without message_identity")

    async def on_batch_data_raw_gnss_measurements(self, data):
        logger.info("Received batch data of raw GNSS measurements")
        rule = f"$aws/rules/raw_data_rule/{self.config.ts_domain_name}/{self.config.device_id}/raw_data"
        logger.info(f"sending gnss data to {rule}, this is the data {data}")
        await self.mqtt_comm.send(rule, data, format="json")



    async def on_mqtt_data_received(self, topic, payload):
        if topic.startswith("/pp/"):
            await self.event_bus.publish("pp_correction_message", payload)
            logger.info(f"Published PP correction data for topic {topic}")
        else:
            logger.info(f"Received MQTT data on topic {topic}")

    async def stop(self):
        logger.info("Cleaning up connections...")
        await self.mqtt_comm.disconnect()
        await self.serial_comm.disconnect()