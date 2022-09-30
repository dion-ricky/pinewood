import logging
from datetime import datetime

import functions_framework

@functions_framework.cloud_event
def main(cloud_event):
    logger = logging.getLogger('main')
    logger.info(f"Executed at {datetime.now()}")