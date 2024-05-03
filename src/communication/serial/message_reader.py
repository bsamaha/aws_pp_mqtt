import logging
from pyubx2 import UBXReader
from src.logger import configure_logging

configure_logging()
logger = logging.getLogger(__name__)

class MessageReader:
    """
    Responsible for reading messages from a stream.
    """
    def read_messages(self, stream, protfilter, publish_raw_data):
        ubx_reader = UBXReader(stream, protfilter=protfilter)
        while stream.in_waiting:
            try:
                raw_data, parsed_data = ubx_reader.read()
                if parsed_data is None:
                    break
                yield (raw_data, parsed_data) if publish_raw_data else parsed_data
            except Exception as e:
                logger.error("Error reading GNSS message: %s", e)