import logging
"""
Configures a logger for the current module.
This code sets up a logger for the current module, enabling logging at the DEBUG level.
It adds a StreamHandler to the logger, which will output log messages to the standard output (console).
"""
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)