import os
import shutil
import tempfile
import posixpath
from datetime import datetime, time, timedelta

import pytz
import functions_framework
from google.cloud.bigquery import (
    LoadJobConfig,
    SourceFormat,
    WriteDisposition
)

from bq import BQClient
from gcs import GCSClient
from config import TABLES
from gdrive import GDriveClient
from utils import sqlite_to_jsonl


@functions_framework.cloud_event
def main(cloud_event):
    execution_date = datetime.combine(datetime.today(), time.min)
    execution_date.replace(tzinfo=pytz.timezone('Asia/Jakarta'))

    temp_dir = os.path.join(tempfile.gettempdir(), 'pinewood')
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)

    keyfile_path = 'secrets/gdrive.json'
    gdrive = GDriveClient(keyfile_path)

    context = {
        'execution_date': execution_date,
        'execution_date_utc': execution_date.astimezone(pytz.UTC),
        'gdrive_client': gdrive,
        'temp_dir': temp_dir,
        'comms': {
            'export': {}, # Comms for passing value between steps
            'upload': {},
            'load': {}
        }
    }

    # Pipeline steps
    accounting_export(context)
    accounting_upload(context)
    accounting_load(context)
    cleanup(temp_dir)


def accounting_export(context):
    print("Begin exporting accounting data")
    execution_date: datetime = context['execution_date']
    yesterday_date = execution_date - timedelta(days=1)
    yesterday_date_s = "{dt.month}-{dt.day}-{year}" \
                        .format(dt=yesterday_date,
                            year=yesterday_date.strftime('%y'))

    output_dir = context['temp_dir']

    re_pattern = r'MMAuto\[.+]\(' + yesterday_date_s + r'-\d+\).mmbak'
    gdrive: GDriveClient = context['gdrive_client']
    file = gdrive.get_file_by_regex(re_pattern)

    output_fn = file.get('name')
    output_path = os.path.join(output_dir, output_fn)
    gdrive.downloader(file.get('id'), output_path)

    db = output_path
    json_output_path = []
    for table in TABLES:
        sql = "SELECT * FROM {}".format(table)
        _output_fn = '{}.json'.format(table.lower())
        _output_path = os.path.join(output_dir, _output_fn)
        json_output_path.append(_output_path)
        sqlite_to_jsonl(db, sql, _output_path)

    context['comms']['export']['output_paths'] = {
        'db': output_path,
        'json_data': json_output_path
    }
    print("Finished exporting accounting data")
    print(context_log_format(context))


def accounting_upload(context):
    print("Begin uploading accounting data to GCS")
    execution_date: datetime = context['execution_date']
    execution_date_s = execution_date.strftime("%Y-%m-%d")

    keyfile_path = 'secrets/pinewood.json'
    gcs_client = GCSClient(keyfile_path)
    storage = gcs_client.client
    bucket_name = 'dionricky-service'

    bucket = storage.bucket(bucket_name)

    # Upload JSON exported data
    json_output_path = []
    for path in context['comms']['export']['output_paths']['json_data']:
        dest_dir = 'pinewood/data/json/{}'.format(execution_date_s)
        dest_fn = os.path.basename(path)
        dest_path = posixpath.join(dest_dir, dest_fn)
        blob = bucket.blob(dest_path)
        json_output_path.append('gs://{}/{}'.format(bucket_name, dest_path))
        blob.upload_from_filename(path)

    # Upload backup DB
    db_output_path = context['comms']['export']['output_paths']['db']

    dest_dir = 'pinewood/data/db'
    dest_fn = '{}.mmbak'.format(execution_date_s)
    dest_path = posixpath.join(dest_dir, dest_fn)
    blob = bucket.blob(dest_path)
    blob.upload_from_filename(db_output_path)

    context['comms']['upload']['output_paths'] = {
        'db': 'gs://{}/{}'.format(bucket_name, dest_path),
        'json_data': json_output_path
    }
    print("Finished uploading accounting data to GCS")
    print(context_log_format(context))


def accounting_load(context):
    print("Begin loading accounting data to BQ")
    keyfile_path = 'secrets/pinewood.json'
    bq_client = BQClient(keyfile_path)
    bigquery = bq_client.client

    for path in context['comms']['upload']['output_paths']['json_data']:
        job_config = LoadJobConfig()
        job_config.source_format = SourceFormat.NEWLINE_DELIMITED_JSON
        job_config.write_disposition = WriteDisposition.WRITE_TRUNCATE
        job_config.autodetect = True

        dest_table = 'dionricky-personal.personal_accounting'
        dest_table = dest_table + '.' + os.path.basename(path).split('.')[0]
        bigquery.load_table_from_uri([path], dest_table, job_config=job_config)
    
    print("Finished loading accounting data to BQ")
    print(context_log_format(context))


def cleanup(temp_dir):
    shutil.rmtree(temp_dir)


def context_log_format(context):
    return {
        'execution_date': context['execution_date'],
        'comms': context['comms']
    }