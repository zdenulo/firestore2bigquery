#! /bin/bash

gcloud functions deploy check-job-status-bq-import \
--project $GCP_PROJECT \
--entry-point cf_check_job_status \
--runtime python37 \
--timeout 540 \
--set-env-vars=GCP_BUCKET=$GCP_BUCKET,PS_JOB_STATUS_TOPIC=$PS_JOB_STATUS_TOPIC,BQ_DATASET=$BQ_DATASET