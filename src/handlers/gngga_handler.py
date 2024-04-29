from datetime import datetime, timezone
import logging
import time
from src.handlers.message_handler import MessageHandler
from src.logger import configure_logging

configure_logging()
logger = logging.getLogger(__name__)

class GNGGAHandler(MessageHandler):
    def process(self, parsed_data, device_id, experiment_id):

        data_dict = {
            "message_type": parsed_data.identity,
            "full_time": datetime.now(tz=timezone.utc).isoformat(),  
            "lat": parsed_data.lat,
            "ns": parsed_data.NS,
            "lon": parsed_data.lon,
            "ew": parsed_data.EW,
            "quality": parsed_data.quality,
            "num_sv": parsed_data.numSV,
            "hdop": parsed_data.HDOP,
            "alt": parsed_data.alt,
            "alt_unit": parsed_data.altUnit,
            "sep": parsed_data.sep,
            "sep_unit": parsed_data.sepUnit,
            "diff_age": parsed_data.diffAge or None,
            "diff_station": parsed_data.diffStation,
            "processed_time": f"{int(time.time()*1000)}",
            "device_id": device_id,
            "experiment_id": experiment_id
        }
        return data_dict
    