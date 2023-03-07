#!/bin/sh

docker build -t checkmate3d.azurecr.io/dev/data-aggregator:latest  -f ./Dockerfile ../..
