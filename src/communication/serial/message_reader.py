import logging
from pyubx2 import UBXReader
from src.logger import configure_logging

configure_logging()
logger = logging.getLogger(__name__)

class MessageReader:
    """
    Responsible for reading messages from a stream.
    """
    def read_messages(self, stream, protfilter):
        ubx_reader = UBXReader(stream, protfilter=protfilter)
        while stream.in_waiting:
            try:
                _, parsed_data = ubx_reader.read()
                if parsed_data is None:
                    break
                yield parsed_data
            except Exception as e:
                logger.error("Error reading GNSS message: %s", e)