"""
Cloud Function which starts collections Firestore export and import to BigQuery when export job is completed
It uses Cloud PubSub to invoke function which checks export job status.
This code is used for 2 separate Cloud Functions
"""

import os
import time
import json
import logging
import base64
from typing import List, Dict

from googleapiclient.discovery import build
from google.cloud import pubsub
from google.cloud import bigquery
import google.auth

GCP_PROJECT = os.environ['GCP_PROJECT']
GCS_BUCKET = os.environ['GCP_BUCKET']
PS_JOB_STATUS_TOPIC = os.environ['PS_JOB_STATUS_TOPIC']

credentials, _project = google.auth.default()
service = build('firestore', 'v1', credentials=credentials, cache_discovery=False)


def publish_pubsub(pubsub_topic: str, message: str):
    """Publishes message to concrete PubSub topic

    :param pubsub_topic: name of pubsub topic
    :param message: pubsub message, string
    """

    ps_topic = f'projects/{GCP_PROJECT}/topics/{pubsub_topic}'
    publisher = pubsub.PublisherClient()
    r = publisher.publish(ps_topic, message.encode())
    r.result()


def export(collection_ids: List[str]) -> str:
    """Create export job for a list of Firestore collections

    :param collection_ids: list of Firebase collection names to export
    :return: export job name
    """

    body = {
        "collectionIds": collection_ids,
        "outputUriPrefix": f"gs://{GCS_BUCKET}"
    }
    name = f'projects/{GCP_PROJECT}/databases/(default)'
    request = service.projects().databases().exportDocuments(name=name, body=body)
    response = request.execute()
    logging.info(response)
    job_name = response['name']
    return job_name


def bq_import(table_name: str, source_uri: str):
    """Import exported Firebase data into BigQuery

    :param table_name: table name into which exported collection will be imported
    :param source_uri: path of metadata file to import
    """

    bq_dataset = os.environ['BQ_DATASET']
    bq = bigquery.Client(project=GCP_PROJECT)
    job_config = bigquery.LoadJobConfig()
    job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.source_format = bigquery.SourceFormat.DATASTORE_BACKUP
    bq_table_path = f"{GCP_PROJECT}.{bq_dataset}.{table_name}"
    table_ref = bigquery.table.TableReference.from_string(bq_table_path)
    bq.load_table_from_uri([source_uri], table_ref, job_config=job_config)


def check_export_job_status(job_name: str):
    """Check status of Firestore export job
    If job is completed, trigger BigQuery imports

    :param job_name - export job name/id
    """

    request = service.projects().databases().operations().get(name=job_name)
    response = request.execute()
    logging.info(response)
    metadata = response['metadata']
    operation_state = metadata['operationState']
    if operation_state == 'SUCCESSFUL':  # Firebase export completed, init BQ import
        collection_ids = metadata['collectionIds']
        gcs_path = metadata['outputUriPrefix']
        for collection_id in collection_ids:
            kind_full_gcs_path = f'{gcs_path}/all_namespaces/kind_{collection_id}/all_namespaces_kind_{collection_id}.export_metadata'
            bq_import(collection_id, kind_full_gcs_path)
    elif operation_state == 'PROCESSING':  # Firebase export still running, wait and check again
        time.sleep(60 * 2)
        data = {
            'name': job_name
        }
        publish_pubsub(PS_JOB_STATUS_TOPIC, json.dumps(data))
    else:
        logging.error(response)


def process(collection_ids):
    job_name = export(collection_ids)
    ps_message = {
        'name': job_name
    }
    publish_pubsub(PS_JOB_STATUS_TOPIC, json.dumps(ps_message))


def cf_init_firestore_export(request):
    """handles incoming request to Cloud Function"""

    collection_ids = request.args.get('collection_ids', '')
    if not collection_ids:
        err = "no collection_ids"
        logging.error(err)
        return err
    collection_ids = collection_ids.split(',')
    job_name = export(collection_ids)
    ps_message = {
        'name': job_name
    }
    publish_pubsub(PS_JOB_STATUS_TOPIC, json.dumps(ps_message))
    return 'ok'


def cf_check_job_status(data, context):
    """triggered by PubSub message

    :param data: PubSub data
    :param context:
    :return:
    """
    data_str = base64.b64decode(data['data']).decode()
    logging.info('received data: {}'.format(data_str))
    try:
        data_json = json.loads(data_str)
    except ValueError:
        err = "couldn't decode data"
        logging.error(err)
        return
    job_name = data_json.get('name', '')
    if not job_name:
        return
    check_export_job_status(job_name)
