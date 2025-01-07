from siametl.logging import Logger

class LoggingHandler:
    def __init__(self):
        pass

    def log_info(self, message):
        Logger.log_info(message)

    def log_error(self, message):
        Logger.log_error(message)

logging_handler = LoggingHandler()
logging_handler.log_info("Ingestion started")
logging_handler.log_error("Failed to read file")
