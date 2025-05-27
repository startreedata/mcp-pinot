"""Centralized logging configuration for the MCP Pinot server."""

import logging
import sys

DEFAULT_LOGGER_NAME = "mcp-pinot"


def setup_logger(name: str = DEFAULT_LOGGER_NAME) -> logging.Logger:
    """
    Set up and configure a logger with console output.
    
    Args:
        name: Logger name
        
    Returns:
        Configured logger instance
    """
    # Get or create logger
    logger = logging.getLogger(name)
    
    # Only configure if not already configured (prevent duplicate handlers)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        
        # Create console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        
        # Create formatter and add it to handler
        formatter = logging.Formatter(
            '%(asctime)s %(name)s %(levelname)s %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(formatter)
        
        # Add console handler to logger
        logger.addHandler(console_handler)
    
    return logger


def get_logger(name: str = DEFAULT_LOGGER_NAME) -> logging.Logger:
    """
    Get a configured logger instance for the specified name.
    If the logger is not already configured by this module's standards,
    it will be set up with the defined console handler and formatter.

    Args:
        name: Logger name

    Returns:
        Configured logger instance
    """
    return setup_logger(name)


# Initialize the main logger when this module is imported
_main_logger = setup_logger() 
