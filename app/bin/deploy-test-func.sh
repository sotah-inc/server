#! /bin/bash

# gathering func dir
FN_DIR=`pwd`/fn/test

# deploying func
gcloud functions deploy HelloHTTP \
    --runtime go111 \
    --trigger-http \
    --source $FN_DIR \
    --region us-central1 # because us-east1 does not fucking have go111 runtime yet