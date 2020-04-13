# Utilities for Firestore export/import to BigQuery

Detailed description is in this blog post [https://www.the-swamp.info/blog/exporting-data-firebase-firestore-bigquery/](https://www.the-swamp.info/blog/exporting-data-firebase-firestore-bigquery/)

## Code explanation
`generate_data.py` - simple script to generate one collection and one subcollection which was used in the blog post as an example  
`export_firestore2bigquery` - basic script to export collection or subcollection from Firestore to BigQuery. It respects the same
scheme as when data is exported through Firebase extension. Unlike official extension's script, this script supports subcollection export.
Use at your own risk :)
`cloud_function` - A code for 2 Cloud Functions, one starts the batch export job, the other one check the job and triggers BigQuery import.
Idea was to use it with Cloud Scheduler and do exports periodically. Don't forget to set up environmental variables for Bash deployment 
scripts.
 
 
