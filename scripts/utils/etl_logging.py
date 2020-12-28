import logging

class pyetl_logger():
    def __init__(self, name, level = 'info', log_frmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'):
        self.name = name
        if (level.lower() == "error"):
            self.level = logging.ERROR
        elif (level.lower() == "debug"):
            self.level = logging.DEBUG
        else:
            self.level = logging.INFO
        self.log_frmt = log_frmt
        self.logger = self.get_logger()

    def get_logger(self):
        logger = logging.getLogger(self.name)
        c_handler = logging.StreamHandler()
        # Create formatters and add it to handlers
        c_format = logging.Formatter(self.log_frmt)
        c_handler.setFormatter(c_format)
        # Add handlers to the logger
        logger.addHandler(c_handler)
        logger.setLevel(self.level)
        return logger

    def info(self, msg):
        self.logger.info(msg)
    
    def error(self, msg):
        self.logger.error(msg)
    
    def debug(self, msg):
        self.logger.debug(msg)
        