#! /bin/bash

# us-central1 because us-east1 does not fucking have go111 runtime yet

# deploying func
gcloud functions deploy LiveAuctionsComputeIntake \
    --runtime go111 \
    --trigger-topic liveAuctionsCompute \
    --source . \
    --region us-central1