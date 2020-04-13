#! /bin/bash

gcloud functions deploy init-export \
--project $GCP_PROJECT \
--entry-point cf_init_firestore_export \
--runtime python37 \
--timeout 540 \
--trigger-http \
--set-env-vars=GCP_BUCKET=$GCP_BUCKET,PS_JOB_STATUS_TOPIC=$PS_JOB_STATUS_TOPIC