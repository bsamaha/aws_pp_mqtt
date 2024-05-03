import asyncio
import logging
from src.logger import configure_logging
from src.config.s2s_config import load_config
from src.edge_device.device_controller import DeviceController
from src.communication.mqtt.aws_mqtt import IoTCoreClient
from src.communication.serial.serial_communication import SerialCommunication  # Assuming SerialCommunication implements CommunicationInterface
from src.edge_device.event_bus import EventBus

configure_logging()
logger = logging.getLogger(__name__)

async def main():
    configure_logging()
    config = load_config()
    mqtt_config = config.mqtt_config()
    serial_config = config.serial_config()

    # Instantiate the event bus
    event_bus = EventBus()

    # Instantiate MQTT and Serial communication interfaces
    mqtt_comm = IoTCoreClient(
        endpoint=mqtt_config["endpoint"],
        port=mqtt_config["port"],
        cert_filepath=mqtt_config["cert_filepath"],
        private_key_filepath=mqtt_config["private_key_filepath"],
        ca_filepath=mqtt_config["ca_filepath"],
        thing_name=mqtt_config["thing_name"],
        pp_region=mqtt_config["pp_region"],
        event_loop=asyncio.get_event_loop(), 
        event_bus=event_bus
    )    
    serial_comm = SerialCommunication(
        port=serial_config["port"],
        baudrate=serial_config["baudrate"],
        timeout=serial_config["timeout"],
        device_id=serial_config["device_id"],
        experiment_id=serial_config["experiment_id"],
        gnss_messages=serial_config["gnss_messages"],
        event_bus=event_bus,
        publish_raw_data=serial_config["publish_raw_data"]
    )

    # Create the device controller with all dependencies
    controller = DeviceController(config, mqtt_comm, serial_comm, event_bus)
    try:
        await controller.start()
        # Keep the program running
        while True:
            await asyncio.sleep(1)
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        logger.info("Stopping controller and cleaning up resources...")
        await controller.stop()

if __name__ == '__main__':
    asyncio.run(main())