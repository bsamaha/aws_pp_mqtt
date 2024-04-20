
import sys
import os

# Add the root project directory to sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)


from serial_communication import SerialCommunication
import asyncio
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# Define the serial port settings
PORT = 'COM10'  # Change this to your serial device's port
BAUDRATE = 38400
TIMEOUT = 1  # Timeout in seconds

# Define your device and experiment identifiers
DEVICE_ID = 'test_script'
EXPERIMENT_ID = 'test_experiment'

# Define the GNSS messages you are interested in
GNSS_MESSAGES = ['GNGGA', 'NAV-PVT']

async def main():
    # Initialize the SerialCommunication class
    serial_comm = SerialCommunication(PORT, BAUDRATE, TIMEOUT, DEVICE_ID, EXPERIMENT_ID, GNSS_MESSAGES)
    
    try:
        # Start reading and sending data
        await serial_comm.read_and_send_data()
    except KeyboardInterrupt:
        # Handle any cleanup here if necessary
        print("Stopping due to keyboard interrupt.")
    finally:
        # Ensure the serial connection is closed properly
        serial_comm.close()

if __name__ == "__main__":
    asyncio.run(main())