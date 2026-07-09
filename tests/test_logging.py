import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from utils.logging_config import setup_logging


def test_setup_logging_returns_logger():
    logger = setup_logging("test_logger_unit")
    assert logger.name == "test_logger_unit"
    assert logger.level == 20  # INFO


def test_setup_logging_idempotent():
    logger1 = setup_logging("test_logger_idempotent")
    handler_count = len(logger1.handlers)
    logger2 = setup_logging("test_logger_idempotent")
    assert logger1 is logger2
    assert len(logger2.handlers) == handler_count
