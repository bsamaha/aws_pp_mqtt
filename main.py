import asyncio
import logging
from src.logger import configure_logging
from src.config.s2s_config import load_config
from src.edge_device.device_controller import DeviceController

# Setup logger for this module
logger = logging.getLogger(__name__)

async def main():
    configure_logging()
    config = load_config()

    controller = DeviceController(config)
    await controller.start()

    try:
        # Keep the program running
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, exiting...")
    finally:
        await controller.stop()

if __name__ == '__main__':
    asyncio.run(main())