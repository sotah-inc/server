#! /bin/bash

# us-central1 because us-east1 does not fucking have go111 runtime yet

# deploying func
gcloud functions deploy SyncItems \
    --runtime go111 \
    --trigger-topic syncItems \
    --source . \
    --region us-central1