import os
import logging

from dotenv import load_dotenv


load_dotenv()


def get_logger(name):
    logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', level=logging.NOTSET)
    logger = logging.getLogger(name)
    logger.setLevel(os.getenv('LOGGING_LEVEL'))
    return logger