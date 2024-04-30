from pydantic import BaseModel, Field
from typing import Set
import os
import glob
from dotenv import load_dotenv
import logging

# Load environment variables from .env file at the root directory
load_dotenv(override=True)

logger = logging.getLogger(__name__)

class CertificateFinder:
    """Handles finding certificate files in a specified directory."""
    def __init__(self, certs_dir='certs/'):
        self.certs_dir = certs_dir

    def get_first_matching_file(self, pattern: str) -> str:
        files = glob.glob(f'{self.certs_dir}{pattern}')
        if not files:
            raise FileNotFoundError(f"No files matching pattern {pattern} found in {self.certs_dir}")
        return files[0]

class EnvironmentConfigLoader:
    """Loads configuration from environment variables."""
    def __init__(self, cert_finder: CertificateFinder):
        self.cert_finder = cert_finder

    def load_env_variables(self) -> dict:
        ca_filepath = self.cert_finder.get_first_matching_file('*AmazonRootCA*.pem')
        private_key_filepath = self.cert_finder.get_first_matching_file('*.pem.key')
        cert_filepath = self.cert_finder.get_first_matching_file('*.pem.crt')

        # split gnss_messages by commas
        gnss_messages = set(os.getenv("GNSS_MESSAGES", "").split(',')) if os.getenv("GNSS_MESSAGES") else set()
        env_variables = {
            "TARGET_EP": os.getenv("TARGET_EP", 'a2u4kg7tuli9al-ats.iot.us-east-1.amazonaws.com'),
            "THING_NAME": os.getenv("THING_NAME", 'blake_test_example'),
            "CERT_FILEPATH": os.getenv("CERT_FILEPATH", cert_filepath),
            "PRIVATE_KEY_FILEPATH": os.getenv("PRIVATE_KEY_FILEPATH", private_key_filepath),
            "CA_FILEPATH": os.getenv("CA_FILEPATH", ca_filepath),
            "SERIAL_PORT": os.getenv("SERIAL_PORT", "COM5"),
            "MQTT_PORT": int(os.getenv("MQTT_PORT", "8883")),
            "BAUDRATE": int(os.getenv("BAUDRATE", "38400")),
            "TIMEOUT": float(os.getenv("TIMEOUT", "1.0")),
            "GNSS_MESSAGES": gnss_messages,  # Use the set here
            "LOGGING_LEVEL": os.getenv("LOGGING_LEVEL", "INFO"),
            "DEVICE_ID": os.getenv("DEVICE_ID", ""),
            "EXPERIMENT_ID": os.getenv("EXPERIMENT_ID", "1"),
            "ALIAS": os.getenv("ALIAS", "blake_test_homeserver"),
            "PP_REGION": os.getenv("PP_REGION", None),
            "TS_DOMAIN_NAME": os.getenv("TS_DOMAIN_NAME", "solutions_team")
        }

        return env_variables

class AppConfig(BaseModel):
    target_ep: str = Field(..., alias="TARGET_EP")
    thing_name: str = Field(..., alias="THING_NAME")
    cert_filepath: str = Field(..., alias="CERT_FILEPATH")
    private_key_filepath: str = Field(..., alias="PRIVATE_KEY_FILEPATH")
    ca_filepath: str = Field(..., alias="CA_FILEPATH")
    mqtt_port: int = Field(..., alias="MQTT_PORT")
    serial_port: str = Field(..., alias="SERIAL_PORT")
    baudrate: int = Field(..., alias="BAUDRATE")
    timeout: float = Field(..., alias="TIMEOUT")
    device_id: str = Field("blake_test_rpi", alias="DEVICE_ID")
    gnss_messages: Set[str] = Field(set(os.getenv("GNSS_MESSAGES", "").split(',')) if os.getenv("GNSS_MESSAGES") else set(), alias="GNSS_MESSAGES")
    logging_level: str = Field("INFO", alias="LOGGING_LEVEL")
    experiment_id: str = Field("1", alias="EXPERIMENT_ID")
    alias: str = Field("blake_test_homeserver", alias="ALIAS")
    pp_region: str = Field("US", alias="PP_REGION")
    ts_domain_name: str = Field("solutions_team", alias="TS_DOMAIN_NAME")

    @classmethod
    def from_env(cls) -> "AppConfig":
        env_variables = EnvironmentConfigLoader.load_env_variables()
        gnss_messages_env = env_variables.get("GNSS_MESSAGES")
        if gnss_messages_env:
            env_variables["GNSS_MESSAGES"] = set(gnss_messages_env.split(","))
        return cls(**env_variables)
    
    def mqtt_config(self) -> dict:
        return {
            "endpoint": self.target_ep,
            "port": self.mqtt_port,
            "cert_filepath": self.cert_filepath,
            "private_key_filepath": self.private_key_filepath,
            "ca_filepath": self.ca_filepath,
            "thing_name": self.thing_name,
            "pp_region": self.pp_region
        }
    
    def serial_config(self) -> dict:
        return {
            "port": self.serial_port,
            "baudrate": self.baudrate,
            "timeout": self.timeout,
            "device_id": self.device_id,
            "experiment_id": self.experiment_id,
            "gnss_messages": self.gnss_messages
        }

def load_config() -> AppConfig:
    cert_finder = CertificateFinder()
    env_loader = EnvironmentConfigLoader(cert_finder)
    env_variables = env_loader.load_env_variables()
    config = AppConfig(**env_variables)
    logger.debug(f"Loaded environment variables: {env_variables}")

    # Set the logging level based on the loaded configuration
    logging_level = config.logging_level.upper()
    logging.getLogger().setLevel(logging_level)
    logger.info(f"Logging level set to {logging_level}")

    return config

    
