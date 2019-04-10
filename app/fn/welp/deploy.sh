#! /bin/bash

# us-central1 because us-east1 does not fucking have go111 runtime yet

# deploying func
gcloud beta functions deploy Welp \
    --runtime go111 \
    --vpc-connector projects/$PROJECT_ID/locations/us-central1/connectors/sotah-connector \
    --env-vars-file .env.yaml \
    --trigger-topic welp \
    --source . \
    --memory 512MB \
    --timeout 500s \
    --region us-central1