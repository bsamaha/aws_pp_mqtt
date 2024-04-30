import logging
import os

def validate_log_level(level):
    """
    Validates and normalizes the log level from environment variables.
    Returns a valid log level.
    """
    valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
    normalized_level = level.upper()
    if normalized_level in valid_levels:
        return normalized_level
    else:
        logging.warning(f"Invalid LOG_LEVEL '{level}'. Defaulting to 'INFO'.")
        return 'INFO'

def configure_logging():
    """
    Configures the global logging settings for the application.
    """
    # Fetch and validate the log level from environment variable
    log_level = validate_log_level(os.getenv('LOG_LEVEL', 'INFO'))

    # Define basic configuration for logging
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Set the validated log level to the root logger
    logging.getLogger().setLevel(log_level)

    # Example for modifying logging level of a third party module
    logging.getLogger('some_third_party_module').setLevel(logging.WARNING)

# Call the function to ensure logging is configured when the module is imported
configure_logging()