#! /bin/bash

# gathering func dir
FN_DIR=`pwd`/fn/live-auctions-intake-compute

# us-central1 because us-east1 does not fucking have go111 runtime yet

# deploying func
gcloud functions deploy LiveAuctionsIntakeCompute \
    --runtime go111 \
    --trigger-topic pricelistHistoriesCompute \
    --source $FN_DIR \
    --region us-central1