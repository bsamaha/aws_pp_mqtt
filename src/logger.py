import logging
import os

def configure_logging():
    """
    Configures the global logging settings for the application.
    """
    # Define basic configuration for logging
    logging.basicConfig(
        level=os.getenv('LOG_LEVEL', 'INFO').upper(),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Example: Adjust log level based on an environment variable
    log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
    logging.getLogger().setLevel(log_level)

    logging.getLogger('some_third_party_module').setLevel(logging.WARNING)

# Call the function to ensure logging is configured when the module is imported
configure_logging()