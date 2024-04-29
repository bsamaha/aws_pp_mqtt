# device_controller.py
import asyncio
import logging
from src.communication.serial.serial_communication import SerialCommunication
from src.communication.mqtt.aws_mqtt import IotCoreBrokerConnection
from src.edge_device.event_bus import EventBus

logger = logging.getLogger(__name__)

class DeviceController:
    def __init__(self, config):
        self.config = config
        self.event_bus = EventBus()
        self.mqtt_comm = IotCoreBrokerConnection(
            endpoint=config.target_ep,
            port=config.mqtt_port,
            cert_filepath=config.cert_filepath,
            private_key_filepath=config.private_key_filepath,
            ca_filepath=config.ca_filepath,
            thing_name=config.thing_name,
            pp_region=config.pp_region,
            event_loop=asyncio.get_running_loop(),
            event_bus=self.event_bus
        )
        self.serial_comm = SerialCommunication(
            port=config.serial_port,
            baudrate=config.baudrate,
            timeout=1,
            device_id=config.alias,
            experiment_id=config.experiment_id,
            gnss_messages=config.gnss_messages,
            event_bus=self.event_bus  # Pass the event bus here
        )

        self.serial_task = None  # Initialize serial_task to None

        # Subscribe to events
        self.event_bus.subscribe("gnss_data", self.on_gnss_data_received)
        self.event_bus.subscribe("mqtt_data_received", self.on_mqtt_data_received)

        # Subscribe to pp_correction_message event from event bus and pass it to serial_comm for handling
        self.event_bus.subscribe("pp_correction_message", self.serial_comm.handle_pp_correction_message)

    async def start(self):
        logger.info("Connecting to MQTT and setting up Serial...")
        try:
            await self.mqtt_comm.connect()
        except Exception as e:
            logger.error(f"Failed to connect to MQTT: {e}")
            await self.cleanup_on_error()
            raise

        try:
            await self.serial_comm.connect()
        except Exception as e:
            logger.error(f"Failed to connect to Serial: {e}")
            await self.cleanup_on_error()
            raise

        logger.info(f"Subscribing to PP region {self.config.pp_region}")
        if self.config.pp_region:
            try:
                await self.mqtt_comm.receive(topic=f"/pp/ubx/0236/Lb", callback=self.on_mqtt_data_received, qos=1)
            except Exception as e:
                logger.error(f"Failed to subscribe to topic /pp/ubx/0236/Lb: {e}")
                await self.cleanup_on_error()
                raise

            try:
                await self.mqtt_comm.receive(topic="/pp/ubx/mga", callback=self.on_mqtt_data_received, qos=1)
            except Exception as e:
                logger.error(f"Failed to subscribe to topic /pp/ubx/mga: {e}")
                await self.cleanup_on_error()
                raise

            try:
                await self.mqtt_comm.receive(topic=f"/pp/Lb/{self.config.pp_region}/+", callback=self.on_mqtt_data_received, qos=0)
            except Exception as e:
                logger.error(f"Failed to subscribe to topic /pp/Lb/{self.config.pp_region}/+: {e}")
                await self.cleanup_on_error()
                raise

    async def cleanup_on_error(self):
        if self.mqtt_comm.is_connected():
            await self.mqtt_comm.disconnect()
        if self.serial_comm.is_connected():
            await self.serial_comm.disconnect()

    async def on_gnss_data_received(self, data):
        # TODO edit this code so that is publishes to the message type specific topic
        # $aws/rules/gnss_data_basic_ingest/{data['message_identity']}/self.
        aws_basic_ingest_rule_for_gnss_data = f"$aws/rules/gngga_basic_ingest/{self.config.ts_domain_name}/{self.config.device_id}"
        logger.debug(f"sending gnss data to {aws_basic_ingest_rule_for_gnss_data}")
        logger.debug(f"Received serial data: {data} ready to send to mqtt broker")
        await self.mqtt_comm.send(aws_basic_ingest_rule_for_gnss_data, data)

    async def on_mqtt_data_received(self, topic, payload):
        if topic in ["/pp/ubx/0236/Lb", "/pp/ubx/mga"] or topic.startswith(f"/pp/Lb/{self.config.pp_region}/"):
            await self.event_bus.publish("pp_correction_message", payload)
            logger.info(f"Published PP correction data to event bus for topic {topic}")
        else:
            logger.info(f"Received MQTT data on topic {topic} not related to PP corrections")

    async def stop(self):
        logger.info("Cleaning up connections...")
        if self.serial_task and not self.serial_task.done():
            self.serial_task.cancel()
            try:
                await self.serial_task  # Wait for the task to be cancelled
            except asyncio.CancelledError:
                pass  # Task cancellation is expected on shutdown
        try:
            await self.mqtt_comm.unsubscribe("/pp/Lb/us/+")
        except asyncio.CancelledError:
            pass  # Handle task cancellation during unsubscribe
        await self.mqtt_comm.disconnect()
        await self.serial_comm.disconnect()
        self.serial_task = None  # Set serial_task to None after cleanup
