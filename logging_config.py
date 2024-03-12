import logging
import os

class UTF8SafeStreamHandler(logging.StreamHandler):
    def emit(self, record):
        log_entry = self.format(record)
        bytes_log_entry = log_entry.encode('utf-8', errors='replace')
        stream = self.stream
        stream.write(bytes_log_entry.decode('utf-8'))
        stream.write(self.terminator)
        self.flush()

def setup_logging(log_filename='application.log', log_level=logging.INFO, log_dir='C:/Users/p7016/Documents/bpa'):
    """
    Set up logging configuration.

    :param log_filename: Name of the log file.
    :param log_level: Logging level.
    :param log_dir: Directory to store log files.
    """
    logger = logging.getLogger()
    logger.setLevel(log_level)

    # Remove all existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Ensure the log_dir exists
    os.makedirs(log_dir, exist_ok=True)

    # Set up formatter
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    
    # File handler
    file_handler = logging.FileHandler(os.path.join(log_dir, log_filename), encoding='utf-8')  # Ensuring UTF-8 encoding for file logs
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Console handler with UTF-8 safe handling
    console_handler = UTF8SafeStreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Set external loggers to WARNING to prevent them from flooding our logs
    logging.getLogger('aiohttp').setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger('asyncio').setLevel(logging.WARNING)

# Example usage:
setup_logging()
logger = logging.getLogger(__name__)