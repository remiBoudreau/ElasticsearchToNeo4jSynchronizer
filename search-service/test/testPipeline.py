#!/usr/bin/env python
from __future__ import print_function
import threading, logging, time, os, sys, uuid
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient
from kafka.errors import UnknownTopicOrPartitionError
import json

sys.path.append('../../sdk')

from util.cloudEventMessage import generate_event, PAYLOAD_TEMPLATE


KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost:29092,localhost:29093,localhost:29094")
INBOUND_TOPIC = os.getenv("SEARCH_SERVICE_INBOUND_TOPIC", "cloud_events")
OUTBOUND_TOPIC = os.getenv("SEARCH_SERVICE_OUTBOUND_TOPIC", "search_service")

producer_stop = threading.Event()
consumer_stop = threading.Event()

class Producer(threading.Thread):
    msg_amount = 1
    msg = generate_event(str(uuid.uuid4()),PAYLOAD_TEMPLATE)

    def run(self):
        producer = KafkaProducer(bootstrap_servers= KAFKA_HOST)
        self.sent = 0

        #while not producer_stop.is_set():
        while self.sent < self.msg_amount:
            producer.send(INBOUND_TOPIC, json.dumps(self.msg).encode('utf-8'))
            self.sent += 1
        producer.flush()


class Consumer(threading.Thread):

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers=KAFKA_HOST,
                                 auto_offset_reset='earliest')
        consumer.subscribe([OUTBOUND_TOPIC])
        self.valid = 0
        self.invalid = 0

        for message in consumer:
            self.valid += 1

            if consumer_stop.is_set():
                break

        consumer.close()

def main():
    threads = [
        Producer(),
        Consumer()
    ]

    for t in threads:
        t.start()

    time.sleep(10)
    producer_stop.set()
    consumer_stop.set()
    print('Messages sent: %d' % threads[0].sent)
    print('Messages recvd: %d' % threads[1].valid)
    
    admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_HOST)
    topic_names=[INBOUND_TOPIC,OUTBOUND_TOPIC]
    
    try:
        admin_client.delete_topics(topics=topic_names)
    except UnknownTopicOrPartitionError as e:
        pass
    except  Exception as e:
        print(e)

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )
    main()