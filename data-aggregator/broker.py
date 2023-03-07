import os
import logging
import datetime
import sys
import orjson as json
sys.path.append('./package')

from util.neo4jConnection import run_query, driver
from queryBuilder import QueryBuilder
from queryExecutor import QueryExecutor
from resultAggregator import ResultAggregator
from permutationsGenerator import PermutationsGenerator

from util.AbstractMessageBrokerHandler import AbstractMessageBrokerHandler, build_inbound_topic_list
from util.cloudEventMessage import generate_event,generate_from_ce,generate_recursive_ce

LOG_USE_STREAM = os.environ.get("LOG_USE_STREAM", True)
LOG_PATH = os.environ.get("LOG_PATH", '/var/log/psi/checkmate')
LOGLEVEL = os.environ.get('LOGLEVEL', 'DEBUG').upper()

logging.basicConfig(level=LOGLEVEL)
logger = logging.getLogger(__name__)
if LOG_USE_STREAM:
    handler = logging.StreamHandler()
else:
    now = datetime.datetime.now()
    handler = logging.FileHandler(
                LOG_PATH 
                + now.strftime("%Y-%m-%d") 
                + '.log')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

CVE_TEMPLATE= {"searchQueries": [                             #computer taxonomy
    {
        "key": "search-id", 
        "value": "dbc33a1c75fd4040b3a241cc495ec868",
        "subject": "Search"
    },
    {
        "key": "taxonomy-id",
        "value": "0fe308b87fd5412c847e0bcef60d6659",
        "subject": "Taxonomy"
    },
    {
        "key": "expansion-query-id",
        "value": "54a1ed7126774488b88034f11db70a90",
        "subject": "ExpansionQuery"
    },
    {
        "key": "name",
        "value": "computer",
        "subject": "NodeType.THING",
        "taxonomy-node-id": "17cd440338cc4e959e0f856e61b031af",
        "properties": [
            {
                "key": "ConstraintType.CONTAINS",
                "value": ["Dell","MSI","Chromebook"],
                "subject": "node",
                "type": "property"
            }],
        "data-source": "CVE"
    },
    {   "key":"name",
        "value": "case",
        "subject": "NodeType.THING",
        "taxonomy-node-id": "8d368a49a5364ae089bf5c6d5e4e8719",
        "properties": [],
        "data-source": "CVE"
    },
    {
    "key":"name",
    "value":"motherboard",
    "subject":"Thing",
    "taxonomy-node-id":"uuid.uuid4.hex()",
    "properties": [{"key":"motherboard-type","value":"","subject":"node","type":"property"}],
    "data-source": "CVE"
},{
    "key":"name",
    "value":"ram",
    "subject":"Thing",
    "taxonomy-node-id":"uuid.uuid4.hex()",
    "properties": [
            {"key": "ConstraintType.EQUALS",
             "value": "LPDDR4",
             "subject": "node",
             "type": "property"}],
    "data-source": "CVE"
},
{
        "key": "tenant-name",
        "value": "MOCKED-ed-will-do-this-uuid",
        "subject": "Tenant"
}
]
}

class DataAggregatorBroker(AbstractMessageBrokerHandler):
    """This is an Example implementation of the AbstracMessageBrokerHandler for testing. 
    For this class to work, the "remove topic" feature must be enables at the kafka testing 
    environment.

    Args:
        AbstractMessageBrokerHandler (_type_): Abstract class to handle the mensage broker.

    Returns:
        _type_: returns a TestMessageBrokerHandler (subclass of util.DataDiscoveryMessageBrokerHandler)
    """
    def __init__(self,bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092,localhost:29093,localhost:29094"),
                 inbound_event = None, 
                 default_outbound_event=os.getenv("KAFKA_DATA_AGGREGATOR_SERVICE_RESULT_UPDATED_EVENT", "resulUpdated"), #outbound_topic,
                 service =os.getenv("KAFKA_DATA_AGGREGATOR_SERVICE", "dataAggregator"),
                 environment = os.getenv('KAFKA_ENVIRONMENT','dev'),
                 group_id = None,
                 max_workers = 16,
                 max_depth =os.environ.get("CHECKMATE_MAX_DEPTH", "1")):
        super().__init__(bootstrap_servers = bootstrap_servers,
                default_inbound_event = inbound_event if inbound_event != None else build_inbound_topic_list(
                        os.getenv("KAFKA_GRAPH_DISCOVERY_SERVICE","graphDiscovery"),
                        [os.getenv("KAFKA_GRAPH_DISCOVERY_SERVICE_GRAPH_UPDATED_EVENT","graphUpdated")],
                        environment
                    ), 
                default_outbound_event = default_outbound_event,
                service = service,
                environment = environment,
                group_id = service if group_id == None else group_id,
                key_prefix=service, 
                max_workers=max_workers) 
        self.query_builder = QueryBuilder()
        self.query_executor = QueryExecutor()
        self.result_aggregator = ResultAggregator()
        self.max_depth = 2 if max_depth==None else int(max_depth)
    

    def handleStream(self, message, event=None, tenant=None, correlationId=None, parentId=None):
        #TODO: Get taxonomy from message
        #if 'taxonomyId' in message:
        #    taxonomy_id = message['taxonomyId']
        #else:
        #    taxonomy_id = '898532bfb1274eaa9ca40c2d239cb1e5'
        taxonomy_id = 'n6dba5b9ec6bb4deb86a4914272487557'
        search_term = None
        if 'parentSearchQuery' in message and message['parentSearchQuery'] != None:
            search_term = message['parentSearchQuery']
        else:
            if 'searchQuery' not in message:
                logger.warn('No search query')
                if 'searchTerm' not in message:
                    logger.warn('No search term')
                else:
                    search_term = message['searchTerm']
            else:
                search_term = message['searchQuery']
        if search_term == None:
            logger.error(f'No "searchQuery" in the payload: {message}')
            return
        print(search_term)
        if 'searchType'in message and message['searchType'] == 'advance':
            #Advanced Search
            print(f'Advance {message["searchType"]}')
            try:
                query, rootNode = self.query_builder.build_advanced_query(taxonomy_id,search_term)
            except Exception as err:
                logger.error(f'Unable to parse advanced search query ({search_term}): {err}')
                return
        else:
            #Simple Search
            print('Simple')
            query, rootNode = self.query_builder.build_query(taxonomy_id,search_term)
            
        #query, rootNode = self.query_builder.build_query(taxonomy_id)
        result = self.query_executor.execute_query(query,rootNode)
        nodes,edges = self.result_aggregator.aggregate(result)
        self.result_aggregator.persist(nodes,edges,message)
        
        # Moved to graph-expansion
        #    
        # if "queryDepth" in message.keys():
        #     queryDepth = int(message["queryDepth"])
        # else:
        #     return
        # if queryDepth >= self.max_depth:
        #     logger.debug(f'Max depth reached ({self.max_depth}) message dropped: {message}')
        #     return
        # else:
        #     queryDepth += 1
        
        # if "previousQueries" not in message:
        #     previousQueries = set()
        # else:
        #     previousQueries = set(message["previousQueries"])
        
        # permutations = self.permutationsGenerator.generate_permutations(nodes,taxonomy_id,search_term)
        # searchQueries = self.permutationsGenerator.build_search(taxonomy_id,permutations)
        
        # for query in searchQueries:
        #     payload = {}
        #     payload['parentSearchQuery'] = search_term
        #     payload['searchQuery'] = search_term
        #     payload['parentId'] = message['correlationId']
        #     payload["searchQueries"] = query.getCloudEventPayload()
        #     payload["queryDepth"] = queryDepth
        #     message = generate_event(payload,parentId=message['correlationId'])
        #     self.produce_output_message(message)
        #
        # return searchQueries
        return None
    
    def stop(self):
        self.consumer.close()

if __name__ == "__main__":
    broker = DataAggregatorBroker()
    logger.info(f"Establised a connection with Kafka...")
    broker.start()

