from datetime import datetime

import pytz

from localpackage.gdrive import GDriveClient

from models.pipeline import Pipeline

from operators.python_operator import PythonOperator

from contrib.accounting import (
    accounting_export,
    accounting_upload,
    accounting_load
)


with Pipeline(
    pipeline_id='accounting',
    schedule='0 12 * * *',
    start_date=datetime(2022, 11, 5, 12, tzinfo=pytz.timezone('Asia/Jakarta'))
) as pipeline:
    keyfile_path = 'secrets/gdrive.json'
    gdrive = GDriveClient(keyfile_path)

    export = PythonOperator(
        task_id='accounting_export',
        python_callable=accounting_export,
        kwargs={'gdrive': gdrive},
        provide_context=True
    )

    upload = PythonOperator(
        task_id='accounting_upload',
        python_callable=accounting_upload,
        provide_context=True
    )

    load = PythonOperator(
        task_id='accounting_load',
        python_callable=accounting_load,
        provide_context=True
    )

    export >> upload >> load