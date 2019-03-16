#! /bin/bash

# us-central1 because us-east1 does not fucking have go111 runtime yet

# deploying func
gcloud functions deploy BootIntake \
    --runtime go111 \
    --trigger-topic bootIntake \
    --source . \
    --memory 128MB \
    --region us-central1