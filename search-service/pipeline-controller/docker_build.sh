#!/bin/sh

docker build -t checkmate3d.azurecr.io/dev/pipeline-controller:latest  -f ./Dockerfile ../..
