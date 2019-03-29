#! /bin/bash

# us-central1 because us-east1 does not fucking have go111 runtime yet

# deploying func
gcloud functions deploy Welp \
    --runtime go111 \
    --trigger-topic welp \
    --source . \
    --memory 512MB \
    --timeout 500s \
    --region us-central1