import sys
from time import sleep
from pathlib import Path

path_root = Path(__file__).parents[2]
sys.path.append(str(path_root))

from models.pipeline import Pipeline

from operators.python_operator import PythonOperator


def _sleep(s, id):
    print(id)
    sleep(s)


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

    t1 >> t2 >> t3