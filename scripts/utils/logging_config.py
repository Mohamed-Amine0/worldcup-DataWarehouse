import logging
import sys

from pythonjsonlogger import jsonlogger


class ContextFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, "context"):
            record.context = {}
        return True


def setup_logging(name: str, log_file: str = None):
    """Configure un logger structuré en JSON pour le monitoring."""
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    logger.addFilter(ContextFilter())

    formatter = jsonlogger.JsonFormatter(
        "%(asctime)s %(name)s %(levelname)s %(message)s %(context)s"
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
