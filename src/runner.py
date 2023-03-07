import sys, os
sys.path.insert(1, '../../sdk/data-ingress-common/')
from dataScraper import DataScraper
from dataIngressMessageBrokerHandler import DataIngressMessageBrokerHandler
from dataSources import DataSources

messageHandler = DataIngressMessageBrokerHandler(DataScraper(), 
                                                DataSources.DATASCRAPER.value,
                                                default_outbound_event=os.getenv("KAFKA_DATA_INGRESS_SERVICE_NEW_UNSTRUCTED_DATA_EVENT", "newUnstructedData"),
                                                environment = os.getenv('KAFKA_ENVIRONMENT','dev'))              
messageHandler.start()
