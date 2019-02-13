#! /bin/sh

gcloud functions deploy HelloHTTP --runtime go111 --trigger-http --source ./fn/test