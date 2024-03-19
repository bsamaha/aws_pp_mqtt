import asyncio
import logging
import os
import sys

# Assuming config, pp_mqtt, and serial_communication are in the same directory or properly installed
import config
import pp_mqtt
from serial_communication import SerialCommunication

# Setup logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.getLevelName(os.getenv('LOGGING_LEVEL', 'INFO').upper()),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)

# Load configuration
config = config.load_config()
print(config)

async def main():
    # Initialize SerialCommunication
    serial_comm = SerialCommunication(
        port=config.port,
        baudrate=config.baudrate,
        timeout=config.timeout,
        device_id=config.device_id,
        gnss_messages=config.gnss_messages,
        experiment_id=config.experiment_id
    )

    # Start the task to read and send data
    asyncio.create_task(serial_comm.read_and_send_data())

    # Initialize and connect MQTT client
    mqtt_client = pp_mqtt.PointPerfectClient(config, serial_comm)
    mqtt_client.connect()

    # Keep the program running to listen for MQTT messages and handle serial communication
    while True:
        await asyncio.sleep(1)  # Sleep to keep the loop running without blocking

if __name__ == "__main__":
    asyncio.run(main())