#! /bin/bash

# us-central1 because us-east1 does not fucking have go111 runtime yet

# deploying func
gcloud functions deploy CleanupIntake \
    --runtime go111 \
    --trigger-topic cleanupCompute \
    --source . \
    --memory 128MB \
    --timeout 500s \
    --region us-central1