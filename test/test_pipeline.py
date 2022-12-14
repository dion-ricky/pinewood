import sys
from time import sleep
from pathlib import Path
from datetime import datetime, timezone

path_root = Path(__file__).parents[1]
sys.path.append(str(path_root))

from models.pipeline import Pipeline
from models.context import Context
from models.comms import Comms

from operators.python_operator import PythonOperator


def _sleep(s, id):
    print(id)
    sleep(s)


Context(
    execution_date=datetime.now(),
    execution_date_utc=datetime.now(timezone.utc),
    temp_dir='./tmp',
    comms=Comms()
)


with Pipeline(
    'test_pipeline',
    schedule='* * * * *',
    start_date=datetime(2022, 11, 7)
) as pipeline:
    t1 = PythonOperator(
        't1',
        python_callable=_sleep,
        provide_context=False,
        args=[1, 't1']
    )

    t2 = PythonOperator(
        't2',
        python_callable=_sleep,
        provide_context=False,
        args=[1, 't2']
    )

    t3 = PythonOperator(
        't3',
        python_callable=_sleep,
        provide_context=False,
        args=[1, 't3']
    )

    t1 >> t2 >> t3

with Pipeline(
    'test_pipeline',
    schedule='* * * * *'
) as pipeline:
    t1 = PythonOperator(
        't1',
        python_callable=_sleep,
        provide_context=False,
        args=[1, 't1']
    )

    t2 = PythonOperator(
        't2',
        python_callable=_sleep,
        provide_context=False,
        args=[1, 't2']
    )

    t3 = PythonOperator(
        't3',
        python_callable=_sleep,
        provide_context=False,
        args=[1, 't3']
    )

    [t1, t2] >> t3