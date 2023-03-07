import logging
import tensorflow as tf
from tensorflow.core.framework import types_pb2
from tensorflow_serving.apis import predict_pb2
from tensorflow_serving.apis import prediction_service_pb2, get_model_metadata_pb2
from tensorflow_serving.apis import prediction_service_pb2_grpc
import grpc
from grpc.beta import implementations

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

class TFServingHandler:
    """
    A class used to handle the requests to a ML model through a gRPC request using the TFServing system.
    ...

    Attributes
    ----------
    model_name : str
        The name of the model to be used.
            
    signature_name : str, optional
        The signature of the model (default is "serving_default")
            
    tfserver_host : str
        The host of the gRPC service (default is "odds-tfserver")
            
    tfserver_port : int, optional
        The host of the gRPC service (default is 8500) 

    Methods
    -------
    get_stub(self):
        Creates insecure (no encryption) gRPC stub (channel) to a host.
    
    get_model_prediction(self, model_input, model_name=None, model_signature=None):
        Uses the created stub to make a gRPC resquest for a Model inference.
    
    """
    
    def __init__(self, model_name, tfserver_host, tfserver_port=8500, model_signature="serving_default"):
        """
        Parameters
        ----------
        model_name : str
        The name of the model to be used.
            
        signature_name : str, optional
            The signature of the model (default is "serving_default")

        tfserver_host : str
            The host of the gRPC service (default is "odds-tfserver")

        tfserver_port : int, optional
            The host of the gRPC service (default is 8500) 
        """
        self.model_name = model_name
        self.tfserver_host = tfserver_host
        self.tfserver_port = tfserver_port
        self.model_signature = model_signature
        self.stub = self.get_stub()
        
    def get_stub(self):
        """Creates a prediction service stub from a given insecure (no encryption) gRPC channel to a host. 
        This method is called in the class constructor.
        
        Parameters
        ----------
        host : str, opitional
            The host of the gRPC service (defautl is self.tfserver_host or None).
            
        port : int, optional
            The host of the gRPC service (default is 8500).
        
        Raises
        -------
        grpc.RpcError
            Raised by the gRPC library to indicate non-OK-status RPC termination.
            
        Returns
        -------
        tensorflow_serving.apis.prediction_service_pb2_grpc.PredictionServiceStub
            The stub.
            
        """
        channel = grpc.insecure_channel(f'{self.tfserver_host}:{self.tfserver_port}')
        stub = prediction_service_pb2_grpc.PredictionServiceStub(channel)
        return stub
    
    def get_model_prediction(self, model_input, model_name=None, model_signature=None):
        """ Uses the created stub to make a gRPC resquest for a Model inference. The input shape must be
        the same as defined in the signature of the model. Both the model and the signature must exist and
        be the deployed on the /models folder of the TFServer container.
        
        Parameters
        ----------
        model_input : dict
            A dictionary (os json Object) containing the same fields of the signature of the model
        
        model_name : str, optional
            The name of the model to be used (default is self.model_name).
            
        signature_name : str, optional
            The signature of the model (default is "serving_default")
            
        Returns
        -------
        dict
            A dictionary containing the result of the inference.
        
        """
        request = predict_pb2.PredictRequest()
        request.model_spec.name = self.model_name if model_name == None else model_name
        request.model_spec.signature_name = self.model_signature if model_signature == None else model_signature

        for key, value in model_input.items():
            request.inputs[key].CopyFrom(tf.make_tensor_proto(value, dtype = tf.float32,shape=[1,1]))

        response = self.stub.Predict.future(request, 10.0)
        output = response.result(timeout=10).outputs
        result={}
        for key,value in output.items():
            result[key]=list(value.float_val)

        return result

