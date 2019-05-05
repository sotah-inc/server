#! /bin/bash

# us-central1 because us-east1 does not fucking have go111 runtime yet

# deploying func
gcloud functions deploy SyncItemIcons \
    --runtime go111 \
    --trigger-topic syncItemIcons \
    --source . \
    --memory 256MB \
    --region us-central1