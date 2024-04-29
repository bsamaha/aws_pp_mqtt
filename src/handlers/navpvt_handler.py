from datetime import datetime, timezone
import logging
from src.handlers.message_handler import MessageHandler

logger = logging.getLogger(__name__)

class NAVPVTHandler(MessageHandler):
    def process(self, parsed_data, device_id, experiment_id):
        logger.info("Processing NAV-PVT message: %s", parsed_data)

        full_time = datetime.now(tz=timezone.utc).isoformat() 
        logger.debug(parsed_data)
        data_dict = {
            "message_type": "NAV-PVT",
            "full_time": full_time,
            "year": parsed_data.year,
            "month": parsed_data.month,
            "day": parsed_data.day,
            "hour": parsed_data.hour,
            "min": parsed_data.min,
            "second": parsed_data.second,
            "validDate": parsed_data.validDate,
            "validTime": parsed_data.validTime,
            "tAcc": parsed_data.tAcc,
            "fixType": parsed_data.fixType,
            "gnssFixOk": parsed_data.gnssFixOk,
            "numSV": parsed_data.numSV,
            "lon": parsed_data.lon,
            "lat": parsed_data.lat,
            "height": parsed_data.height,
            "hMSL": parsed_data.hMSL,
            "hAcc": parsed_data.hAcc,
            "vAcc": parsed_data.vAcc,
            "velN": parsed_data.velN,
            "velE": parsed_data.velE,
            "velD": parsed_data.velD,
            "gSpeed": parsed_data.gSpeed,
            "headMot": parsed_data.headMot,
            "sAcc": parsed_data.sAcc,
            "headAcc": parsed_data.headAcc,
            "pDOP": parsed_data.pDOP,
            "device_id": device_id,
            "experiment_id": experiment_id
        }
        return data_dict