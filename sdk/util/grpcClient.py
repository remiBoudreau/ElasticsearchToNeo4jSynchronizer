import grpc
import os
import sys
sys.path.append('./torchserve')

import inference_pb2
import inference_pb2_grpc
import management_pb2
import management_pb2_grpc
import logging

LOG_USE_STREAM = True
LOG_PATH = os.environ.get("LOG_PATH", '/var/log/psi/checkmate')

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
if LOG_USE_STREAM:
    handler = logging.StreamHandler()
else:
    now = datetime.datetime.now()
    handler = logging.FileHandler(
                LOG_PATH 
                + now.strftime("%Y-%m-%d") 
                + '.log')
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def get_inference_stub(host='localhost'):
    channel = grpc.insecure_channel(f'{host}:7070')
    stub = inference_pb2_grpc.InferenceAPIsServiceStub(channel)
    return stub


def get_management_stub(host='localhost'):
    channel = grpc.insecure_channel(f'{host}:7071')
    stub = management_pb2_grpc.ManagementAPIsServiceStub(channel)
    return stub


def infer(stub, model_name, model_input):
    input_data = {'data': model_input.encode('utf-8')}
    response = stub.Predictions(
        inference_pb2.PredictionsRequest(model_name=model_name,
                                         input=input_data))
    try:
        prediction = response.prediction.decode('utf-8')
        return prediction
    except grpc.RpcError as e:
        logger.error(f'GRPC error due to: {e}')
        exit(1)

def infer_file(stub, model_name, model_input):
    with open(model_input, 'rb') as f:
        data = f.read()

    input_data = {'data': data}
    response = stub.Predictions(
        inference_pb2.PredictionsRequest(model_name=model_name,
                                         input=input_data))

    try:
        prediction = response.prediction.decode('utf-8')
        return prediction
    except grpc.RpcError as e:
        logger.error(f'GRPC error due to: {e}')
        exit(1)


def register(stub, model_name, mar_set_str):
    mar_set = set()
    if mar_set_str:
        mar_set = set(mar_set_str.split(','))
    marfile = f"{model_name}.mar"
    logger.info(f"## Check {marfile} in mar_set :", mar_set)
    if marfile not in mar_set:
        marfile = "https://torchserve.s3.amazonaws.com/mar_files/{}.mar".format(
            model_name)

    logger.info(f"## Register marfile:{marfile}\n")
    params = {
        'url': marfile,
        'initial_workers': 1,
        'synchronous': True,
        'model_name': model_name
    }
    try:
        response = stub.RegisterModel(
            management_pb2.RegisterModelRequest(**params))
        print(f"Model {model_name} registered successfully")
    except grpc.RpcError as e:
        print(f"Failed to register model {model_name}.")
        print(str(e.details()))
        exit(1)


def unregister(stub, model_name):
    try:
        response = stub.UnregisterModel(
            management_pb2.UnregisterModelRequest(model_name=model_name))
        print(f"Model {model_name} unregistered successfully")
    except grpc.RpcError as e:
        print(f"Failed to unregister model {model_name}.")
        print(str(e.details()))
        exit(1)


if __name__ == '__main__':
    # args:
    # 1-> api name [infer, register, unregister]
    # 2-> model name
    # 3-> model input for prediction
    args = sys.argv[1:]
    if args[0] == "infer":
        infer_file(get_inference_stub(), args[1], args[2])
    else:
        api = globals()[args[0]]
        if args[0] == "register":
            api(get_management_stub(), args[1], args[2])
        else:
            api(get_management_stub(), args[1])