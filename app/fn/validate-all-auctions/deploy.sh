#! /bin/bash

# us-central1 because us-east1 does not fucking have go111 runtime yet

# deploying func
gcloud functions deploy ValidateAllAuctions \
    --runtime go111 \
    --trigger-topic validateAllAuctions \
    --source . \
    --memory 128MB \
    --timeout 500s \
    --region us-central1