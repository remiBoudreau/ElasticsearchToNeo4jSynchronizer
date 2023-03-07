from confluent_kafka import Producer, Consumer, KafkaException

from datetime import datetime
import os
from configparser import ConfigParser
import logging
import orjson as json
from abc import ABCMeta, abstractmethod
from threading import Event, Thread, RLock

from util.cloudEventMessage import generate_from_ce


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

def json_serializer(v):
    """ This funciton serialize the Kafka message contents from JSON to bytes

        Parameters
        ----------
        v : dict
            A dict representation of a JSON.

        Raises
        ----------
        json.decoder.JSONDecodeError
            If it is unable to decode the JSON or the string is not JSON compliant.

        Returns
        ----------
        byte
            The byte represention of a stringified JSON onject.

    """
    if v is None:
        return
    try:
        return json.dumps(v)
    except json.JSONDecodeError:
        logger.error('Unable to decode: %s', v)
        return None


def json_deserializer(v):
    """ This funciton deserialize the Kafka message contents from bytes to JSON

        Parameters
        ----------
        v : byte
            A byte representation of a string containing a JSON.

        Raises
        ----------
        json.decoder.JSONDecodeError
            If it is unable to decode the JSON or the string is not JSON compliant.

        Returns
        ----------
        json.Object
            A JSON onject.

    """
    if v is None:
        return
    try:
        return json.loads(v)
    except json.JSONDecodeError:
        logger.error('Unable to decode: %s', v)
        return v.decode('utf-8')
    
class AbstractMessageBrokerHandler(metaclass=ABCMeta):
    """
    A class used to handle Kafka messages from Message Broker.

    ...

    Attributes
    ----------
    consumer: KafkaConsumer
        The Kafka broker to consume data.
        
    producer: KafkaProducer
        The Kafka broker to send data.
    
    key_prefix: str, optional
        The prefix of the produced Kafka key. If None, the produced message won't have key, otherwise, it will be
        the prefix concatenated to the orignal key (default is None)
    
    max_workers: int, optional
        The maximum number of workers to consume Kafka Messages. If it is set to a value <= 0, it won't use parallel
        processing and won't auto-commit messages. The commit will occur after processing the messages (default is 8).
        
    Methods
    -------

    start():
        This method starts the handler. The handler starts consuming the Kafka topic, combining and amalgamating 
        the sources, and producing the results to the outbound Kafka topic.
    """
    def __init__(self, 
                default_inbound_event, 
                default_outbound_event,
                service,
                bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS','pkc-56d1g.eastus.azure.confluent.cloud:9092'),
                environment = os.getenv('KAFKA_PRODUCTION_ENVIRONMENT','checkmated'),
                group_id = 'checkmate', 
                key_prefix=None, 
                max_workers=16):
        """
        Parameters
        ----------
        
        """
        
        self.default_outbound_event = default_outbound_event
        self.key_prefix = key_prefix
        self.max_workers = max_workers
        self.running_workers = 0
        self.environment = environment
        self.service = service
        
        #self._max_workers_event = None

        sasl_conf = {
            'sasl.mechanism': 'PLAIN',
            'security.protocol': 'SASL_SSL',
            'sasl.username': os.getenv('KAFKA_SASL_USERNAME','UD7DUM6NGGSWAQTY'),
            'sasl.password': os.getenv('KAFKA_SASL_PASSWORD','wfjVn2/23+U8RqYhurgQSgLvRJZYMp1QXrKDV/GlshhUCCMPJvdj8lcTYKizowKl'),
            #'sasl.jaas.config': 'org.apache.kafka.common.security.plain.PlainLoginModule required', 
        }

        consumer_conf = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'enable.auto.commit': True if max_workers > 0 else False,
            'auto.offset.reset': 'latest' #'earliest'
        }
        consumer_conf.update(sasl_conf)
        self.consumer = Consumer(consumer_conf)
        
        self.consumer.subscribe([default_inbound_event])
        
        logger.debug('Created KafkaConsumer')
        
        producer_conf = {
            'bootstrap.servers': bootstrap_servers,
        }
        producer_conf.update(sasl_conf)
        self.producer = Producer(producer_conf)
        
        logger.debug('Created KafkaProducer')
    
    def _kafka_delivery_report(self, err, msg):
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        if err is not None:
            logger.error('Message delivery failed: {} [key:{}]'.format(err, msg.key()))
        #else:
        #    logger.debug('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
    
    def _consume_message(self, message):
        """ This function is called when a Kafka Message arrives.
        
        Parameters
        ----------
        message: confluent_kafka.Message
            A kafka message (refer to https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.Message)
        """
        message.set_value(json_deserializer(message.value()))
        logger.debug(f'Message received:{message.value()}')
        tenant = message.topic().split('.')[1]
        result = self.handleRawStream(message)
        if result:
            if type(result) == list:
                for r in result:    
                    self.produce_output_message(payload=r,tenant=tenant,
                                                 original_message = message)
            else:
                self.produce_output_message(payload=result, tenant=tenant,
                                             original_message = message)
        if self.max_workers > 0:
            self.running_workers -= 1
            #self._max_workers_event.set()
            self.consumer.commit(message)
                    
    def produce_output_message(self, payload, tenant, original_message=None,event=None):
        """This function is called after processing a message to produce the Kafka message to the next layer.

        Parameters
        ----------
            original_message: confluent_kafka.Message
                The message received by the layer inf first places from each 
                the fields of the generatad message is supossed to copy the values.
            
            payload: (dict)
                A dictionary containing the representation of the ouptput JSON (cloudevent payload).
        """
        
        if event == None:
            event = self.default_outbound_event

        outbound_topic = f'{self.environment}.{tenant}.{self.service}.{event}'
        logger.debug(f'Outbound Topic:{outbound_topic}')
        # Trigger any available delivery report callbacks from previous produce() calls
        self.producer.poll(0)
        
        logger.debug(f'Output payload:{payload}')
        if original_message != None:
            result = generate_from_ce(original_message.value(), payload)
        else:
            result = payload
        
        logger.debug(f'Message output:{result}')
        message_key = 'None'
        
        try:
            # Asynchronously produce a message. The delivery report callback will
            # be triggered from the call to poll() above, or flush() below, when the
            # message has been successfully delivered or failed permanently.
            if self.key_prefix != None and original_message != None and original_message.key() != None:
                message_key = original_message.key().decode("utf-8")
                if ":" in message_key:
                    message_key = message_key.split(":", 1)[1]
                message_key = f'{self.key_prefix}:{message_key}'
                # self.producer.send(
                #     self.outbound_topic, 
                #     key=message_key.encode('utf-8'), 
                #     value=result)
                self.producer.produce(outbound_topic, json_serializer(result), message_key.encode('utf-8'), callback=self._kafka_delivery_report)
            else:
                self.producer.produce(outbound_topic, json_serializer(result), callback=self._kafka_delivery_report)
                
            # Wait for any outstanding messages to be delivered and delivery report
            # callbacks to be triggered.
            self.producer.flush()
            logger.debug(f'Message sent (key: {message_key})')
        except KafkaException as err:
            logger.error(f'Unable to produce kafka message due to kafka error ({err}): {result}')
        except Exception as err:
            logger.error(f'Unable to produce kafka message due to some unexpected error({err}): {result}')
        
    def start(self):
        """ This method block the current thread by starting to consuming the messages from the specified inbound 
            Kafka topic, and producing the results to the outbound Kafka topic.
        """
        
        try:
            #self._available_workers_event = Event()
            while True:
                if self.running_workers > self.max_workers:
                    logger.debug(f'Max workers reached ({self.max_workers})')
                    #self._available_workers_event.clear()
                    #self._available_workers_event.wait()
                    while self.running_workers > self.max_workers:
                        continue
                        
                message = self.consumer.poll(1)

                if message is None:
                    continue
                if message.error():
                    logger.error("Consumer error: {}".format(message.error()))
                    continue
                logger.debug(f'Message received (offset: {message.offset()})')
            
                if self.max_workers > 0:
                    self.running_workers += 1
                    
                    thread = Thread(target=self._consume_message, args=(message,), daemon=True)
                    thread.start()
                else:
                    self._consume_message(message)
        except KafkaException as error:
            logger.error(f' KAFKAEXCEPTION {error} on the consumer.')
        except Exception as error:
            logger.error(f' EXCEPTION {error} on the consumer.')
        finally:
            logger.error(f'CLOSING consumer {self.consumer} ')
            self.consumer.close()
            return 1 

    def handleRawStream(self, rawMessage):
        payload = json.loads(bytes(rawMessage.value()['data']['value']))
        logger.debug(f'Received payload:{payload}')
        if 'correlationId' not in payload:
            if 'extensions' in rawMessage.value():
                payload['correlationId'] = rawMessage.value()['extensions']['correlationid']
            else:
                logger.warn("No extensions in the Could Event Header.")
                payload['correlationId'] = rawMessage.value()['id']
        if 'parentId' in rawMessage.value()['extensions']:
            payload['parentId'] = rawMessage.value()['extensions']['parentId']
        else:
            payload['parentId'] = None
        try:
            topic = rawMessage.topic()
            splittedTopic = topic.split('.')
            result = self.handleStream(payload, splittedTopic[3], splittedTopic[1], payload['correlationId'], parentId=payload['parentId'])
        except Exception as err:
            logger.exception(f"Error processing message ({err}).")
            #TODO encode payload in case of error
            result=payload

        return result

    @abstractmethod
    def handleStream(self, message, event, tenant, correlationId, parentId=None):
        """ This method receives a Kafka message from the Kafka Consumer and returns na Object/String that will be 
        serialized to the Kafka producer
        
        Parameters
        ----------
        message : dict
            A dicitionary containing the payload of the received cloud event.
        
        """
        return NotImplemented
