import os
import logging
import datetime
import requests
from requests.exceptions import ConnectionError,Timeout,HTTPError
import sys
sys.path.append('./package')
from package.dataSources import DataSources
from package.pattern import Pattern
from package.search import Search
from package.constraintType import ConstraintType
import joblib
from package.nodeConstraint import NodeConstraint
from package.nodeType import NodeType
import copy

from package.expansionQuery import ExpansionQuery
from util.AbstractMessageBrokerHandler import AbstractMessageBrokerHandler, build_inbound_topic_list

LOG_USE_STREAM = True
LOG_PATH = os.environ.get("LOG_PATH", '/var/log/psi/checkmate')
LOGLEVEL = os.environ.get('LOGLEVEL', 'INFO').upper()

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

CVE_TEMPLATE= {"searchQueries": [                             #taxonomy_root_node taxonomy
    {
        "key": "search-id", 
        "value": "dbc33a1c75fd4040b3a241cc495ec868",
        "subject": "Search"
    },
    {
        "key": "taxonomy-id",
        "value": "898532bfb1274eaa9ca40c2d239cb1e5",
        "subject": "Taxonomy"
    },
    {
        "key": "expansion-query-id",
        "value": "54a1ed7126774488b88034f11db70a90",
        "subject": "ExpansionQuery"
    },
    {
        "key": "name",
        "value": "taxonomy_root_node",
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

class SearchServiceMessageBroker(AbstractMessageBrokerHandler):
    """This is an Example implementation of the AbstracMessageBrokerHandler for testing. 
    For this class to work, the "remove topic" feature must be enables at the kafka testing 
    environment.

    Args:
        AbstractMessageBrokerHandler (_type_): Abstract class to handle the mensage broker.

    Returns:
        _type_: returns a TestMessageBrokerHandler (subclass of util.DataDiscoveryMessageBrokerHandler)
    """
    def __init__(self, bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092,localhost:29093,localhost:29094"),
                 inbound_event = None, 
                 default_outbound_event=os.getenv("KAFKA_PIPELINE_CONTROLER_SERVICE_SEARCH_GENERATED_EVENT", "searchGenerated"), #outbound_topic,
                 service =os.getenv("KAFKA_PIPELINE_CONTROLER_SERVICE", "pipelineController"),
                 environment = os.getenv('KAFKA_ENVIRONMENT','dev'),
                 group_id = None,
                 max_workers=1):
        super().__init__( 
                bootstrap_servers = bootstrap_servers,
                default_inbound_event = inbound_event if inbound_event != None else build_inbound_topic_list(
                        os.getenv("KAFKA_CATALOG_SERVICE","catalog"),
                        [os.getenv("KAFKA_CATALOG_SERVICE_CLOUD_EVENT","newSearch")],
                        environment
                    ), 
                default_outbound_event = default_outbound_event,
                service = service,
                environment = environment,
                group_id = service if group_id == None else group_id,
                key_prefix=service, 
                max_workers=max_workers)
        
    def build_advanced_search(self, taxonomy_id, search_id,filters=None):
        taxonomy = joblib.load(f"taxonomies/taxonomy_{taxonomy_id}.joblib")
        taxonomy_root_node = taxonomy.getStartNode()
        
        constraints = filters.split("AND")[1:] # TODO get entity type instead of [1:]
        
        ncs = []
        for constraint in constraints:
            key, value = constraint.split(':')
            key = key.strip()
            value = value.strip()
            if key == 'email':
                # TODO this should come in the search payload
                EMAIL_ID = 'n68e4e7ae6b9a475198a5c21ef4149098'
                
                nc =NodeConstraint(affectedNodeId=EMAIL_ID,
                    nodeType= NodeType.EMAIL.value,
                    constrainedAttributeName='name',
                    constraintType=ConstraintType.EQUALS,
                    constraintValue=value)
            else:
                nc =NodeConstraint(affectedNodeId=taxonomy_root_node.getId(),
                    nodeType= taxonomy_root_node.getAttribute("NodeType"),
                    constrainedAttributeName=key,
                    constraintType=ConstraintType.STARTSWITH,
                    constraintValue=value)
            ncs.append(nc)
        
        # for filter in filters:
        #     if filter['key'] == 'email':
        #         # TODO this should come in the search payload
        #         EMAIL_ID = 'n68e4e7ae6b9a475198a5c21ef4149098'
                
        #         nc =NodeConstraint(affectedNodeId=EMAIL_ID,
        #             nodeType= NodeType.EMAIL.value,
        #             constrainedAttributeName=filter['key'],
        #             constraintType=ConstraintType.STARTSWITH,
        #             constraintValue=filter['value'])
        #     else:
        #         nc =NodeConstraint(affectedNodeId=taxonomy_root_node.getId(),
        #             nodeType= taxonomy_root_node.getAttribute("NodeType"),
        #             constrainedAttributeName=filter['key'],
        #             constraintType=ConstraintType.STARTSWITH,
        #             constraintValue=filter['value'])
        #     ncs.append(nc)
        s = Search(taxonomy_id,ncs,[])
        s.id = search_id
        return s
    
    def build_search(self, taxonomy_id, search_id,searchQuery=None):
        a_taxonomy = joblib.load(f"taxonomies/taxonomy_{taxonomy_id}.joblib")
        taxonomy_root_node = a_taxonomy.getStartNode()
        if searchQuery == None or searchQuery == '':
            nc =NodeConstraint(affectedNodeId=taxonomy_root_node.getId(),
                    nodeType= taxonomy_root_node.getAttribute("NodeType"),
                    constrainedAttributeName="name",
                    constraintType=ConstraintType.STARTSWITH,
                    constraintValue="\'t\'")
        else:
            nc =NodeConstraint(affectedNodeId=taxonomy_root_node.getId(),
                    nodeType= taxonomy_root_node.getAttribute("NodeType"),
                    constrainedAttributeName="name",
                    constraintType=ConstraintType.STARTSWITH,
                    constraintValue=searchQuery)

        s = Search(taxonomy_id,[nc],[])
        s.id = search_id
        return s
    
    def post_airflow_job(self,cypherQuery):
        token = os.getenv("AIRFLOW_API_KEY","")
        base_domain = os.getenv("AIRFLOW_BASE_DOMAIN","airflow-webserver")
        port = os.getenv("AIRFLOW_PORT","8080")
        deployment_name = os.getenv("AIRFLOW_DEPLOYMENT_RELEASE_NAME","checkmate3d")
        dag_id = os.getenv("CYPHER_DAG","cypher_dag")
        
        data = {"cypherQuery": cypherQuery}
        
        try:
            resp = requests.post(
                url=f"http://{base_domain}:{port}/api/v1/dags/{dag_id}/dagRuns",
                headers={"Authorization": token, "Content-Type": "application/json"},
                auth=('airflow', 'airflow'),
                json={"conf": data}
            )
            if not resp:
                logger.error(f'Airflow API returned an error: {resp.status_code}')
        except ConnectionError as e:
            logger.error(f'Airflow connection error: {e}')
        except HTTPError as e:
            logger.error(f'Airflow HTTP error: {e}')
        except Timeout as e:
            logger.error(f'Airflow timeout error: {e}')

    def handleStream(self, message, event=None, tenant=None, correlationId=None, parentId=None):
        #TODO: Get taxonomy from message
        #if 'taxonomyId' in message:
        #    taxonomy_id = message['taxonomyId']
        #else:
        #    taxonomy_id = '898532bfb1274eaa9ca40c2d239cb1e5'
        
        if "queryDepth" not in message.keys() or message["queryDepth"]==None:
            logger.error('No queryDepth in message')
            queryDepth="0"
        else:
            queryDepth = message["queryDepth"]
            
        # taxonomy_id = '898532bfb1274eaa9ca40c2d239cb1e5' # taxonomy_root_node taxonomy
        taxonomy_id = 'n6dba5b9ec6bb4deb86a4914272487557' #person taxonomy_id
        
        #if 'filters' in message and len(message['filters']) > 0:
        #    #Advanced Search
        #    search = self.build_advanced_search(taxonomy_id,message['searchId'],message['filters'])
        #elif 'searchType'in message and message['searchType'] == 'advance':
        if 'searchType'in message and message['searchType'] == 'advance':
            #Advanced Search
            try:
                search = self.build_advanced_search(taxonomy_id,message['searchId'],message['searchQuery'])
            except Exception as err:
                logger.error(f'Unable to parse advanced search query ({message["searchQuery"]}): {err}')
                return
        else:
            #Simple Search
            search = self.build_search(taxonomy_id,message['searchId'],message['searchQuery'])
                
        dataSources = [DataSources.PEOPLEDATALABS.value,
                       DataSources.DATASCRAPER.value,
                       DataSources.EMAILBREACHDETECTOR.value,
                       # DataSources.COAUTHORSEARCH.value,
                       ]
        
        queries = []
        cypherQuery = None
        for dataSource in dataSources:
            expansionQueries,cypherQuery=search.knowledgeGraphDiscoveryQuery([dataSource])
            queries+=expansionQueries
        
        self.post_airflow_job(cypherQuery)

        result = []
        for eq in queries:
            r = copy.deepcopy(message)
            r['queryDepth'] = queryDepth
            r["searchQueries"] = eq.getCloudEventPayload()
            result.append(r)
        #result = [copy.deepcopy(message)["searchQueries"] = eq.getCloudEventPayload() for eq in expansionQueries]
        
        return result

if __name__ == "__main__":
    broker = SearchServiceMessageBroker()
    logger.info(f"Establised a connection with Kafka...")
    broker.start()

