import logging


def confgure_logging(name='pbp_logger'):
    # Create the logger if it doesn't exist
    logger = logging.getLogger(name)

    # Check if handlers are already present
    if not logger.handlers:
        # Set the logging level
        logger.setLevel(logging.INFO)

        # Create handlers for different logging levels
        error_handler = logging.StreamHandler()
        info_handler = logging.StreamHandler()

        # Set the logging level for each handler
        error_handler.setLevel(logging.ERROR)
        info_handler.setLevel(logging.INFO)

        # Create formatters for different logging formats
        error_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s - Line %(lineno)d - %(message)s')
        info_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        # Set the formatters for each handler
        error_handler.setFormatter(error_formatter)
        info_handler.setFormatter(info_formatter)

        # Add the handlers to the logger
        logger.addHandler(error_handler)
        logger.addHandler(info_handler)

    return logger


