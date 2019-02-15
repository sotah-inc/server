#! /bin/bash

# gathering func dir
FN_DIR=`pwd`/fn/test2

# deploying func
gcloud functions deploy HelloPubSub \
    --runtime go111 \
    --trigger-topic boot \
    --source $FN_DIR \
    --region us-central1 # because us-east1 does not fucking have go111 runtime yet