import logging


def get_logger(name):
    """
    Create a logger with the specified name and set up handlers and formatters.
    """
    # Create a custom logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Create handlers
    console_handler = logging.StreamHandler()

    # Set the level for handlers
    console_handler.setLevel(logging.INFO)

    # Create formatters and add them to handlers
    console_format = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(console_format)

    # Add handlers to the logger
    logger.addHandler(console_handler)

    return logger
