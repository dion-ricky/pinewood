import os
import sys
import tokenize
from pathlib import Path
from datetime import datetime, timezone

path_root = Path(__file__).parents[1]
sys.path.append(str(path_root))

from models.context import Context
from models.comms import Comms


Context(
    execution_date=datetime.now(),
    execution_date_utc=datetime.now(timezone.utc),
    temp_dir='./tmp',
    comms=Comms()
)

pipelines = []

for dirpath, _dirnames, filenames in os.walk('./test/pipeline'):
        for fn in filenames:
            fpath = os.path.join(dirpath, fn)
            if os.path.splitext(fn)[1] == '.py' and os.path.isfile(fpath):
                with tokenize.open(fpath) as f:
                    pipeline = compile(f.read(), fn, 'exec')
                    pipelines.append(pipeline)

for pipeline in pipelines:
    exec(pipeline)