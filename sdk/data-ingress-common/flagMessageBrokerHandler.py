import os,datetime, logging, sys
sys.path.insert(1, '../')#'../sdk/' for docker /notebooks/PSI/sdk/
from util.AbstractMessageBrokerHandler import AbstractMessageBrokerHandler, build_inbound_topic_list
from utils import loggerFunction
# from dataScraper import DataScraper
# from peopleDataLabs import PeopleDataLabs
# from emailBreachDetector import EmailBreachDetector
# from cveDataFetcher import CVEDataFetcher                             # Uncomment when datasources are added
# from coAuthorSearch import CoAuthorSearch
# from socialMediaExtractor import SocialMediaExtractor         

     
logger = loggerFunction()

class FlagMessageBrokerHandler(AbstractMessageBrokerHandler):
    """Data Ingress broker Handler.
    
    Example payload (computer taxonomy):
    
    {
        ...
        "correlationId": "dbc33a1c75fd4040b3a241cc495ec868",
        ...
        "searchQueries": [                            
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
                "language" : "English",
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
                "language" : "English",
                "taxonomy-node-id": "8d368a49a5364ae089bf5c6d5e4e8719",
                "properties": [],
                "data-source": "CVE"
            },
            {
                "key":"name",
                "value":"motherboard",
                "subject":"Thing",
                "language" : "English",
                "taxonomy-node-id":"uuid.uuid4.hex()",
                "properties": [{"key":"motherboard-type","value":"","subject":"node","type":"property"}],
                "data-source": "CVE"
            },{
                "key":"name",
                "value":"ram",
                "subject":"Thing",
                "language" : "English",
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
    """
    def __init__(self, 
                 dataFetcher,
                 flagName,
                 bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092,localhost:29093,localhost:29094"),
                 default_inbound_event = build_inbound_topic_list(
                                                    os.getenv("KAFKA_DATA_INGRESS_SERVICE","dataIngress"),
                                                    [
                                                        os.getenv("KAFKA_DATA_INGRESS_SERVICE_NEW_UNSTRUCTED_DATA_EVENT","newUnstructedData"),
                                                        os.getenv("KAFKA_DATA_INGRESS_SERVICE_UPDATE_UNSTRUCTED_DATA_EVENT","updateUnstructedData"),
                                                        os.getenv("KAFKA_DATA_INGRESS_SERVICE_NEW_STRUCTED_DATA_EVENT","newStructedData"),
                                                        os.getenv("KAFKA_DATA_INGRESS_SERVICE_UPDATE_STRUCTED_DATA_EVENT","updateStructedData")
                                                    ],
                                                    os.getenv('KAFKA_ENVIRONMENT','dev')
                                                ), 
                 default_outbound_event=os.getenv("KAFKA_DATA_INGRESS_SERVICE_NEW_FLAG_EVENT", "newFlag"), #outbound_topic,
                 service =os.getenv("KAFKA_DATA_INGRESS_SERVICE","data-ingress"),
                 environment = os.getenv('KAFKA_ENVIRONMENT','dev'),
                 max_workers=1
                 ) -> None:
        super().__init__(
                    bootstrap_servers = bootstrap_servers,
                    default_inbound_event = default_inbound_event, 
                    default_outbound_event = default_outbound_event,
                    service = service,
                    environment = environment,
                    group_id = flagName, 
                    key_prefix=service, 
                    max_workers=max_workers
                    )
        self.dataFetcher = dataFetcher
        
        
    def handleStream(self, message, event=None, tenant=None, correlationId=None, parentId=None):
        '''
        This method gets the cloudevent from the "DATA_INGRESS_INBOUND_TOPIC" and shares it with "DataFetcher" abstract class for query purposes.
        Note:
        args:
            Takes any additional arguments required by the datasource function. E.g. for PDL, args can be peopleSearchStatus.
        '''
       
        # dataSourceName = None
        # for content in message.keys():
        #     if content == "searchQueries":
        #         for jsonContent in message[content]:
        #             if jsonContent["key"] == "name":
        #                 dataSourceName = jsonContent["data-source"]
            
        # if dataSourceName == None:
        #     logger.error('Not Data-source found in the payload: {queryMessage}')
        #     return None
        
        # try:
        #     language = message["searchQueries"][4]["language"]
        # except KeyError:
        #     language = "English"
            
        # responseMessage = message
        
        searchQuery = message.get("searchQuery", None)
        if searchQuery == None:
            logger.warn('No searchQuery in the input payload.')
            
        parentSearchQuery = message.get("parentSearchQuery", None)
        correlationId = message.get("correlationId", None)
        parentId = message.get("parentId", None)
        queryDepth = message.get("queryDepth", None)
        searchType = message.get("searchType", None)
  
        responseMessage = None
        responseMessage = self.dataFetcher.startProcess(message)
        logger.info(f'dataFetcher: {self.dataFetcher}')
        # else:
        #     logger.debug(f'Payload data source ({dataSourceName} different from container data source ({self.dataSourceType}))')

        if responseMessage == None:
            logger.warning(f'Unable to process cloud event: {message}')
            return
        
        if searchQuery != None:
            if type(responseMessage) == list:
                for message in responseMessage:
                    message['searchQuery'] = searchQuery
                    message['parentSearchQuery'] = parentSearchQuery
                    message['correlationId'] = correlationId
                    message['parentId'] = parentId
                    message['queryDepth'] = queryDepth
                    message['searchType'] = searchType
            else:
                responseMessage['searchQuery'] = searchQuery
                responseMessage['parentSearchQuery'] = parentSearchQuery
                responseMessage['correlationId'] = correlationId
                responseMessage['parentId'] = parentId
                responseMessage['queryDepth'] = queryDepth
                responseMessage['searchType'] = searchType
        
        return responseMessage