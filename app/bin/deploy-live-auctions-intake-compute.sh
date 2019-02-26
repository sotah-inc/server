#! /bin/bash

# gathering func dir
FN_DIR=`pwd`/fn/live-auctions-compute-intake

# us-central1 because us-east1 does not fucking have go111 runtime yet

# deploying func
gcloud functions deploy LiveAuctionsComputeIntake \
    --runtime go111 \
    --trigger-topic liveAuctionsCompute \
    --source $FN_DIR \
    --region us-central1