"""Centralized logging configuration for the MCP Pinot server."""

import atexit
import logging
import sys

DEFAULT_LOGGER_NAME = "mcp-pinot"


class SafeStreamHandler(logging.StreamHandler):
    """
    A StreamHandler that safely handles closed streams in containerized environments.
    """

    def __init__(self, stream=None):
        super().__init__(stream)
        self._closed = False

    def emit(self, record):
        # Completely avoid writing if we know the stream is closed
        if self._closed or not self.stream or self.stream.closed:
            return

        try:
            # Use the parent's emit method but catch all possible errors
            super().emit(record)
        except (ValueError, OSError, AttributeError, BrokenPipeError):
            # Mark as closed and silently ignore future attempts
            self._closed = True
            pass

    def close(self):
        """Override close to mark as closed."""
        self._closed = True
        try:
            super().close()
        except (ValueError, OSError):
            pass


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

        # Create safe console handler for containerized environments
        try:
            console_handler = SafeStreamHandler(sys.stdout)
            console_handler.setLevel(logging.INFO)

            # Create formatter and add it to handler
            formatter = logging.Formatter(
                "%(asctime)s %(name)s %(levelname)s %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
            console_handler.setFormatter(formatter)

            # Add console handler to logger
            logger.addHandler(console_handler)
        except (ValueError, OSError):
            # Fallback to basic logging if stdout is not available
            logging.basicConfig(
                level=logging.INFO,
                format="%(asctime)s %(name)s %(levelname)s %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
                force=True
            )

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


def cleanup_logging():
    """Safely cleanup logging handlers to prevent errors during shutdown."""
    try:
        # Get all loggers and close their handlers
        for logger_name in logging.Logger.manager.loggerDict:
            logger = logging.getLogger(logger_name)
            for handler in logger.handlers[:]:
                try:
                    handler.close()
                    logger.removeHandler(handler)
                except (ValueError, OSError):
                    # Ignore errors during cleanup
                    pass
    except Exception as e:
        # Log any errors during cleanup but don't raise
        # Use a more robust approach to avoid nested try-except-pass
        logger = logging.getLogger()
        if logger.handlers:
            logger.warning(f"Error during logging cleanup: {e}")
        # If logging fails, we can't do anything about it during cleanup


# Register cleanup function to run on exit
atexit.register(cleanup_logging)

# Initialize the main logger when this module is imported
_main_logger = setup_logger()
