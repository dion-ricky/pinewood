import os
import json
import posixpath
from datetime import datetime, timedelta

from google.cloud.bigquery import (
    LoadJobConfig,
    SourceFormat,
    WriteDisposition
)

from localpackage.bq import BQClient
from localpackage.gcs import GCSClient
from localpackage.config import TABLES
from localpackage.gdrive import GDriveClient
from localpackage.utils import sqlite_to_jsonl
from localpackage.utils import logging

from models.context import Context


logger = logging.get_logger(__name__)


def accounting_export(gdrive, context: Context):
    print("Begin exporting accounting data")
    execution_date = context.execution_date
    yesterday_date = execution_date - timedelta(days=1)
    yesterday_date_s = "{dt.month}-{dt.day}-{year}" \
                        .format(dt=yesterday_date,
                            year=yesterday_date.strftime('%y'))

    output_dir = context.temp_dir

    re_pattern = r'MMAuto\[.+]\(' + yesterday_date_s + r'-\d+\).mmbak'
    gdrive: GDriveClient = gdrive
    file = gdrive.get_file_by_regex(re_pattern)

    output_fn = file.get('name')
    output_path = os.path.join(output_dir, output_fn)
    gdrive.downloader(file.get('id'), output_path)

    db = output_path
    json_output_path = []
    for table in TABLES:
        logger.debug(f"Exporting table {table}")
        sql = "SELECT * FROM {}".format(table)
        _output_fn = '{}.json'.format(table.lower())
        _output_path = os.path.join(output_dir, _output_fn)
        logger.debug(f"Finished exporting {table} to {_output_path}")
        json_output_path.append(_output_path)
        sqlite_to_jsonl(db, sql, _output_path)

    context.comms.push('accounting_export', 'output_paths', {
        'db': output_path,
        'json_data': json_output_path
    })
    logger.info("Finished exporting accounting data")


def accounting_upload(context: Context):
    print("Begin uploading accounting data to GCS")
    config = json.load(open('config/accounting_config.json'))

    execution_date: datetime = context.execution_date
    execution_date_s = execution_date.strftime("%Y-%m-%d")

    keyfile_path = 'secrets/pinewood.json'
    gcs_client = GCSClient(keyfile_path)
    storage = gcs_client.client
    bucket_name = config.get('upload_bucket')

    bucket = storage.bucket(bucket_name)

    # Upload JSON exported data
    json_output_path = []
    for path in context.comms.pull('accounting_export', 'output_paths')['json_data']:
        logger.debug(f"Uploading {path}")
        dest_dir = 'pinewood/data/json/{}'.format(execution_date_s)
        dest_fn = os.path.basename(path)
        dest_path = posixpath.join(dest_dir, dest_fn)
        blob = bucket.blob(dest_path)
        json_output_path.append('gs://{}/{}'.format(bucket_name, dest_path))
        blob.upload_from_filename(path)
        logger.debug(f"Finished uploading {path} to {bucket_name}")

    # Upload backup DB
    db_output_path = context.comms.pull('accounting_export', 'output_paths')['db']

    dest_dir = 'pinewood/data/db'
    dest_fn = '{}.mmbak'.format(execution_date_s)
    dest_path = posixpath.join(dest_dir, dest_fn)
    blob = bucket.blob(dest_path)
    blob.upload_from_filename(db_output_path)

    context.comms.push('accounting_upload', 'output_paths', {
        'db': 'gs://{}/{}'.format(bucket_name, dest_path),
        'json_data': json_output_path
    })
    logger.debug(f"DB backup path {'gs://{}/{}'.format(bucket_name, dest_path)}")
    logger.debug(f"JSON data in GCS {json_output_path}")
    logger.info("Finished uploading accounting data to GCS")


def accounting_load(context: Context):
    print("Begin loading accounting data to BQ")
    config = json.load(open('config/accounting_config.json'))

    keyfile_path = 'secrets/pinewood.json'
    bq_client = BQClient(keyfile_path)
    bigquery = bq_client.client

    for path in context.comms.pull('accounting_upload', 'output_paths')['json_data']:
        logger.debug(f"Loading {path} to BigQuery")
        job_config = LoadJobConfig()
        job_config.source_format = SourceFormat.NEWLINE_DELIMITED_JSON
        job_config.write_disposition = WriteDisposition.WRITE_TRUNCATE
        job_config.autodetect = True

        dest_project_id = config.get("project_id")
        dest_dataset = config.get("dataset_id")
        dest_table = f"{dest_project_id}.{dest_dataset}"
        dest_table = dest_table + '.' + os.path.basename(path).split('.')[0]
        bigquery.load_table_from_uri([path], dest_table, job_config=job_config)
        logger.debug(f"Finished loading {path} to {dest_table}")
    
    logging.info("Finished loading accounting data to BQ")