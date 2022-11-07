import os
import shutil
import tempfile
import tokenize
from base64 import b64decode
from datetime import datetime
from collections import deque

import pytz
import functions_framework

from models.context import Context
from models.comms import Comms

from localpackage.utils.logging import get_logger


logger = get_logger(__name__)


@functions_framework.cloud_event
def main(cloud_event):
    data = cloud_event.data
    logger.debug(data.get('message', 'No message'))
    _main(data)


def _main(data):
    message = b64decode(data['message']['data']).decode('utf-8')

    execution_date = datetime.now()

    if message != 'OK':
        execution_date = datetime.strptime(message, '%Y-%m-%d %H:%M:%S')

    execution_date = execution_date.replace(tzinfo=pytz.timezone('Asia/Jakarta'))

    logger.debug(f"Execution date: {execution_date.strftime('%Y-%m-%d %H:%M:%S')}")

    temp_dir = os.path.join(tempfile.gettempdir(), 'pinewood')
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)

    Context(
        execution_date = execution_date,
        execution_date_utc = execution_date.astimezone(pytz.UTC),
        temp_dir = temp_dir,
        comms = Comms()
    )

    pipelines = deque()

    for dirpath, _dirnames, filenames in os.walk('./pipeline'):
        for fn in filenames:
            fpath = os.path.join(dirpath, fn)
            if os.path.splitext(fn)[1] == '.py' and os.path.isfile(fpath):
                logger.debug(f"Found pipeline at {fpath}")
                with tokenize.open(fpath) as f:
                    pipeline = compile(f.read(), fn, 'exec')
                    pipelines.append(pipeline)

    for pipeline in pipelines:
        logger.debug("Executing pipeline")
        exec(pipeline)

    cleanup(temp_dir)


def cleanup(temp_dir):
    logger.debug(f"Deleting {temp_dir}")
    shutil.rmtree(temp_dir)