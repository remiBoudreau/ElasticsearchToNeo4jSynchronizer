#!/bin/sh
pip install -U grpcio protobuf grpcio-tools

python3 -m grpc_tools.protoc --proto_path=./proto/ --python_out=. --grpc_python_out=. 