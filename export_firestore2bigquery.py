"""
Exports sub(collection) from Firestore to BigQuery in format compatible with Firebase extension.
it's run by command:
python export_firestore2bigquery <arguments>

table name and schema to which data is exported is compatible with extension, i.e. <(sub)collection name>_raw_changelog
it needs to exists in advance

developed with Python 3.7
"""

import json
import argparse
import datetime
from google.cloud import firestore
from google.cloud import bigquery


def export(gcp_project: str, dataset_name: str, kind_name: str, batch_size: str, subcollection=False) -> None:
    """goes through all documents in (sub)collection in Firestore and uploads to BigQuery table

    :param dataset_name - name od BigQuery dataset where documents will be uploaded
    :param kind_name - name of (sub)collection which will be exported
    :param subcollection - whether collection or subcollection is exported
    """

    db = firestore.Client(project=gcp_project)
    bq = bigquery.Client(project=gcp_project)

    c = 0
    table_name = f'{kind_name}_raw_changelog'
    dataset_ref = bq.dataset(dataset_name)
    dataset = bigquery.Dataset(dataset_ref)
    table_ref = dataset.table(table_name)
    table = bigquery.Table(table_ref)
    table = bq.get_table(table)
    ref = db.collection(kind_name)
    if subcollection:
        ref = db.collection_group(kind_name)

    docs = ref.stream()
    to_upload = []
    for doc in docs:
        doc_data = doc.to_dict()
        doc_data_json = json.dumps(doc_data)
        doc_path = doc.reference.path
        row = {
            'timestamp': datetime.datetime.utcnow(),
            'event_id': '',
            'document_name': f'projects/{gcp_project}/databases/(default)/documents/{doc_path}',
            'operation': 'IMPORT',
            'data': doc_data_json
        }
        to_upload.append(row)
        if len(to_upload) == batch_size:
            res = bq.insert_rows(table, to_upload)
            c += len(to_upload)
            print(f"so far {c}")
            to_upload = []

    if to_upload:
        res = bq.insert_rows(table, to_upload)
        c += len(to_upload)
        print(f"total: {c}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--gcp_project", help="GCP project ID", required=True)
    parser.add_argument("--dataset_name", help="Name of BigQuery dataset where data will be exported, needs to exist",
                        required=True)
    parser.add_argument("--kind_name", help="Firebase (sub)collection name", required=True)
    parser.add_argument("--batch_size", help="Number of documents to upload to BigQuery", default=100)
    parser.add_argument("--subcollection", default=False,
                        help="Check (add arbitrary value if you are importing subcollection, by default it expects collection")
    args = parser.parse_args()
    subcollection = False
    if args.subcollection:
        subcollection = True
    export(args.gcp_project, args.dataset_name, args.kind_name, args.batch_size, subcollection)
