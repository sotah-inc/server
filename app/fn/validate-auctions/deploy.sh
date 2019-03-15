#! /bin/bash

# us-central1 because us-east1 does not fucking have go111 runtime yet

# deploying func
gcloud functions deploy ValidateAuctions \
    --runtime go111 \
    --trigger-topic validateAuctions \
    --memory 256MB \
    --source . \
    --region us-central1