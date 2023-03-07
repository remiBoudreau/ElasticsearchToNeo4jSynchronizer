import uuid
import unittest
import os
from util.AbstractMessageBrokerHandler import AbstractMessageBrokerHandler, json_deserializer, json_serializer
from util.cloudEventMessage import generate_event, PAYLOAD_TEMPLATE
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient
from kafka.errors import UnknownTopicOrPartitionError
import multiprocessing
import time
import orjson as json


class TestMessageBrokerHandler(AbstractMessageBrokerHandler):
    """This is an Example implementation of the AbstracMessageBrokerHandler for testing. 
    For this class to work, the "remove topic" feature must be enables at the kafka testing 
    environment.

    Args:
        AbstractMessageBrokerHandler (_type_): Abstract class to handle the mensage broker.

    Returns:
        _type_: returns a TestMessageBrokerHandler (subclass of util.AbstractMessageBrokerHandler)
    """
    
    def __init__(self, bootstrap_servers, inbound_topic, outbound_topic, key_prefix, is_external_docker_network=True):
        super().__init__(bootstrap_servers, inbound_topic, outbound_topic,
                         key_prefix=key_prefix, is_external_docker_network=is_external_docker_network)
        
    def handleStream(self, message):
        #result = message.value
        #result_json = json.loads(message.value)
        print(message)
        return message
    
    def stop(self):
        self.consumer.close()

INBOUND_TOPIC1 = "inboud_test1"
OUTBOUND_TOPIC1 = "outbound_test1"
TEST_TOPIC = "outbound_test2"
KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost:29092,localhost:29093,localhost:29094")
CLIENT_ID = "test"
MESSAGE_KEY_PREFIX = "test"

class TestMessageBroker(unittest.TestCase):
    """Class with to test the message broker pipeline.

    Args:
        unittest (_type_): This class uses python unittest library.

    Returns:
        _type_: returns a TestMessageBroker (subclass of unittest.TestCase)
    """

    def setUp(self):
        """Set ups a new test by creating a KafkaAdmin that will be used to cleanup the
        test environmnet.
        """
        self.process = None
        self.admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_HOST)

    def test_topics(self):
        """This method veryfies if the envrionment variables containing the topics
        are correctly connected to each other.

        """
        self.assertEqual(os.getenv("INBOUND_TOPIC"),
                         os.getenv("DATA_INGRESS_INBOUND_TOPIC"))
        self.assertEqual(os.getenv("DATA_INGRESS_OUTBOUND_TOPIC"),
                         os.getenv("DATA_ENRICHMENT_INBOUND_TOPIC"))
        self.assertEqual(os.getenv("DATA_ENRICHMENT_OUTBOUND_TOPIC"), os.getenv(
            "DATA_PROCESS_PERSIST_INBOUND_TOPIC"))
        self.assertEqual(os.getenv("DATA_PROCESS_PERSIST_OUTBOUND_TOPIC"), os.getenv(
            "TRANSFORMATION_MAPPING_INBOUND_TOPIC"))
        self.assertEqual(os.getenv("TRANSFORMATION_MAPPING_OUTBOUND_TOPIC"), os.getenv(
            "DATA_DISCOVERY_INBOUND_TOPIC"))
        self.assertEqual(os.getenv("DATA_DISCOVERY_OUTBOUND_TOPIC"), os.getenv(
            "GRAPH_DISCOVERY_INTBOUND_TOPIC"))
        self.assertEqual(os.getenv("GRAPH_DISCOVERY_OUTBOUND_TOPIC"),
                         os.getenv("GRAPH_EXPANSION_INBOUND_TOPIC"))
        self.assertEqual(os.getenv("GRAPH_EXPANSION_OUTBOUND_TOPIC"),
                         os.getenv("GRAPH_POPULATION_INBOUND_TOPIC"))
        self.assertEqual(os.getenv("GRAPH_POPULATION_OUTBOUND_TOPIC"),
                         os.getenv("DATA_AGGREGATION_INBOUND_TOPIC"))
    
    def test_messageBrokerHandler(self):
        """This method tests if the an instance fof the AbstractMessageBrokerHandler 
        (TestMessageBrokerHandler) class is able to receive and foward messages
        """
        producer = KafkaProducer(bootstrap_servers=KAFKA_HOST,
                                retries=5,
                                max_block_ms=30000,
                                value_serializer=json_serializer
                                )
        consumer = KafkaConsumer(bootstrap_servers=KAFKA_HOST,
                                group_id=None,
                                consumer_timeout_ms=30000,
                                auto_offset_reset='earliest',
                                value_deserializer=json_deserializer
                                )
        
        futures = []
        
        messages = 10
        
        def listen_message1():
            messageHandler = TestMessageBrokerHandler(
                KAFKA_HOST, INBOUND_TOPIC1, OUTBOUND_TOPIC1, MESSAGE_KEY_PREFIX)
            messageHandler.start()
                # self.messageHandler.process.terminate()
        
        process = multiprocessing.Process(target=lambda: listen_message1())
        process.start()
        time.sleep(2)
        
        for i in range(messages):
            test_message = generate_event(
                clientId='1234',
                json_payload=json.loads(f'{{\"data\":\"Test Message {i}\"}}')
            )
            futures.append(producer.send(INBOUND_TOPIC1, key=str(uuid.uuid4()).encode(
                'utf-8'), value=test_message))
        
        ret = [f.get(timeout=30) for f in futures]
        assert len(ret) == messages
        producer.close()
        
        consumer.subscribe([OUTBOUND_TOPIC1])
        msgs = set()
        for i in range(messages):
            try:
                msgs.add(json.loads(next(consumer).value['data'])['data'])
            except StopIteration:
                break
        
        consumer.close()
        process.terminate()
        assert msgs == set([f'Test Message {i}' for i in range(messages)])
        self.admin_client.delete_topics(topics=[INBOUND_TOPIC1,OUTBOUND_TOPIC1])
        

    def test_kafka(self):
        """This method tests if the Kafka brokers are up and running.
        """
        producer = KafkaProducer(bootstrap_servers=KAFKA_HOST,
                                retries=5,
                                max_block_ms=30000,
                                value_serializer=str.encode
                                )
        consumer = KafkaConsumer(bootstrap_servers=KAFKA_HOST,
                                group_id=None,
                                consumer_timeout_ms=30000,
                                auto_offset_reset='earliest',
                                value_deserializer=bytes.decode
                                )

        messages = 100
        futures = []
        for i in range(messages):
            futures.append(producer.send(TEST_TOPIC, 'msg %d' % i))
        ret = [f.get(timeout=30) for f in futures]
        assert len(ret) == messages
        producer.close()

        consumer.subscribe([TEST_TOPIC])
        msgs = set()
        for i in range(messages):
            try:
                msgs.add(next(consumer).value)
            except StopIteration:
                break
        assert msgs == set(['msg %d' % (i,) for i in range(messages)])
        consumer.close()
        self.admin_client.delete_topics(topics=[TEST_TOPIC])
        
    def test_full_pipeline(self):
        """This method tests the full pipeline (end-to-end)
        """
        
        def listen_message(inbound_topic, outbound_topic, prefix):
            messageHandler = TestMessageBrokerHandler(
                KAFKA_HOST, inbound_topic, outbound_topic, prefix)
            messageHandler.start()
            
        self.process0 = multiprocessing.Process(target=lambda: listen_message(
            os.getenv("DATA_INGRESS_INBOUND_TOPIC","cloud_events"),
            os.getenv("DATA_INGRESS_OUTBOUND_TOPIC","data_ingress"),
            "data_ingress"
            ))
        self.process0.start()
        
        self.process1 = multiprocessing.Process(target=lambda: listen_message(
            os.getenv("DATA_ENRICHMENT_INBOUND_TOPIC","data_ingress"),
            os.getenv("DATA_ENRICHMENT_OUTBOUND_TOPIC","data_enrichment"),
            "data_enrichment"
            ))
        self.process1.start()
        
        self.process2 = multiprocessing.Process(target=lambda: listen_message(
            os.getenv("DATA_PROCESS_PERSIST_INBOUND_TOPIC","data_enrichment"),
            os.getenv("DATA_PROCESS_PERSIST_OUTBOUND_TOPIC","data_process_persist"),
            "data_process_persist"
            ))
        self.process2.start()
        
        self.process3 = multiprocessing.Process(target=lambda: listen_message(
            os.getenv("TRANSFORMATION_MAPPING_INBOUND_TOPIC","data_process_persist"),
            os.getenv("TRANSFORMATION_MAPPING_OUTBOUND_TOPIC","transform_mapping"),
            "transform_mapping"
            ))
        self.process3.start()

        self.process4 = multiprocessing.Process(target=lambda: listen_message(
            os.getenv("DATA_DISCOVERY_INBOUND_TOPIC","transform_mapping"),
            os.getenv("DATA_DISCOVERY_OUTBOUND_TOPIC","data_discovery"),
            "transform_mapping"
            ))
        self.process4.start()
        
        self.process5 = multiprocessing.Process(target=lambda: listen_message(
            os.getenv("GRAPH_DISCOVERY_INBOUND_TOPIC","data_discovery"),
            os.getenv("GRAPH_DISCOVERY_OUTBOUND_TOPIC","graph_discovery"),
            "graph_expansion"
            ))
        self.process5.start()
        
        self.process6 = multiprocessing.Process(target=lambda: listen_message(
            os.getenv("GRAPH_EXPANSION_INBOUND_TOPIC","graph_discovery"),
            os.getenv("GRAPH_EXPANSION_OUTBOUND_TOPIC","graph_expansion"),
            "graph_expansion"
            ))
        self.process6.start()
        
        self.process7 = multiprocessing.Process(target=lambda: listen_message(
            os.getenv("GRAPH_POPULATION_INBOUND_TOPIC","graph_expansion"),
            os.getenv("GRAPH_POPULATION_OUTBOUND_TOPIC","graph_population"),
            "graph_population"
            ))
        self.process7.start()

        self.process8 = multiprocessing.Process(target=lambda: listen_message(
            os.getenv("DATA_AGGREGATION_INBOUND_TOPIC","graph_population"),
            os.getenv("DATA_AGGREGATION_OUTBOUND_TOPIC","data_aggregation"),
            "data_aggregation"
            ))
        self.process8.start()
        
        time.sleep(2)
        
        producer = KafkaProducer(bootstrap_servers=KAFKA_HOST,
                                retries=5,
                                max_block_ms=30000,
                                value_serializer=json_serializer
                                )
        consumer = KafkaConsumer(bootstrap_servers=KAFKA_HOST,
                                group_id=None,
                                consumer_timeout_ms=30000,
                                auto_offset_reset='earliest',
                                value_deserializer=json_deserializer
                                )
        
        test_message = generate_event(
                clientId='1234',
                json_payload=json.loads(f'{{\"data\":\"Test Message\"}}')
            )
        
        future = producer.send(os.getenv("DATA_INGRESS_INBOUND_TOPIC","cloud_events"),
                               key=str(uuid.uuid4()).encode('utf-8'), 
                               value=test_message)
        
        future.get(timeout=30)
        producer.close()
        
        consumer.subscribe([os.getenv("DATA_AGGREGATION_OUTBOUND_TOPIC","data_aggregation")])
        
        msg = next(consumer).value['data']
        self.assertEqual(msg, 'Test Message')
        
        consumer.close()
        
        self.process0.terminate()
        self.process1.terminate()
        self.process2.terminate()
        self.process3.terminate()
        self.process4.terminate()
        self.process5.terminate()
        self.process6.terminate()
        self.process7.terminate()
        self.process8.terminate()

    def tearDown(self) -> None:
        """This method clears the environment by *REMOVING* all the topics.
        """
        
        topic_names=[TEST_TOPIC, 
                     INBOUND_TOPIC1,
                     OUTBOUND_TOPIC1,
                     os.getenv("DATA_INGRESS_INBOUND_TOPIC","data_ingress"),
                    ]
        try:
            self.admin_client.delete_topics(topics=topic_names)
        except UnknownTopicOrPartitionError as e:
            pass
        except  Exception as e:
            print(e)
        #self.messageHandler.stop()
        return super().tearDown()
        
if __name__ == '__main__':
    unittest.main()
