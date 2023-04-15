import sys, os
sys.path.insert(1, '../../sdk/data-ingress-common/')
sys.path.insert(1, '../../sdk/')

from dataFetcher import DataFetcher
from Neo4jHandler import Neo4jHandler
from ElasticSearchHandler import ElasticSearchHandler

from utils import loggerFunction
from util.neo4jConnection import Neo4jConnection, GraphDatabase
from util.openmetadataApi import OpenMetadataConnection, retrieveCatalogEntity  
from package.nodeType import NodeType

from typing import List, Dict, Generator, Any

class GrayHatWarfare(DataFetcher):
    def __init__(self) -> None:
        """
        Constructor method for the class.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        super().__init__()
        self.neo4j = Neo4jConnection()
        self.openMeta = OpenMetadataConnection()

        self.parameters: Dict[str, any] = {"properties": ['name'],
                           "thresholds": {
                                'vendor': 0.9,
                                'relatedPersons': 0.9,
                                'relatedOrganizations': 0.9,
                                'amount': 0.9
                                },
                            "types": {
                                'vendor': 'person', 
                                'relatedPersons': 'person', 
                                'relatedOrganizations': 'organization', 
                                }

                            }
        self.neo4jParameters: Dict[str, any] = {
                    "from": ["vendor"],
                    "fromProps": [],
                    "to": ['relatedPersons', 'relatedOrganizations'], 
                    "toProps": [], 
                    "relationship": "HAS_PROVIDED_BUSINESS_TO", 
                    "relationshipProps": ['amount']
                    }
        self.logger = loggerFunction()
    
    
    def elasticsearchQueryBuilder(self, queryCloudEvent: Dict[str, any]) -> Dict[str, any]:
        """
        This method takes a cloud event with a search query and builds an Elasticsearch query using the search parameters.

        Parameters
        ----------
        queryCloudEvent : dict
            A dictionary containing the search parameters.

        Returns
        -------
        search_query : dict
            A dictionary containing the Elasticsearch query parameters.
        """
        selectedProperties: List[str] = self.parameters["selectedProperties"]
        typeProperties: List[str] = list(set(self.parameters.get('types', {}).values()))
        searchProperties: List[Dict[str, any]] = [eventSearchQuery['properties'] for eventSearchQuery in queryCloudEvent.get('searchQueries', [])]
        try:
            queries = [{
                "multi_match": {
                    "query": searchProperty.get('value').lower(),
                    "fields": typeProperties,
                    "operator": "and",
                    "fuzziness": "AUTO"
                }
            } for searchProperty in searchProperties
            if searchProperty.get('subject', '') in selectedProperties]
            # Combine the must queries with a bool query
            searchQuery = {"bool": {"must": queries}} if queries else {}
        except Exception as e:
            self.logger.error(f"'An error occurred in elasticQueryBuilder function: {str(e)}'", exc_info=True)
            return None
        else:
            return searchQuery

    def neo4jQueryBuilder(self, dataFetchResponse: Dict[str, any]) -> Generator[Dict[str, any]]:
        """
        This function generates nodes and edges for Neo4j graph database using the Elasticsearch response data.

        Parameters
        ----------
        dataFetchResponse : dict
            A dictionary containing the search results from Elasticsearch.

        Yields
        ------
        dict
            A dictionary containing the data required to create nodes and edges in Neo4j database.
        """
        try:
            for doc in self.docGenerator(dataFetchResponse):
                for fromNode in [entityKey.lower() for entityKey in self.neo4jParameters.get('from', [])]:
                    for toNode in [entityKey.lower() for entityKey in self.neo4jParameters.get('to', [])]:
                            yield {
                                'fromType': self.parameters.get('types', {fromNode: []}).get(fromNode),
                                'fromProps': {'name': doc[fromNode], **{propKey: doc[propKey] for propKey in self.neo4jParameters.get('fromProps', []) if propKey != "name"}} ,
                                'toType': self.parameters.get('types', {toNode: []}).get(toNode),
                                'toProps':{'name': doc[toNode], **{propKey: doc[propKey] for propKey in self.neo4jParameters.get('toProps', []) if propKey != "name"}},
                                'edgeType': self.parameters.get('relationship'),
                                'edgeProps': {key: doc[key] for key in self.neo4jParameters['relationshipProps']}
                            }
        except Exception as e:
            self.logger.error(f"'An error occurred in neo4jQueryBuilder function: {str(e)}'", exc_info=True)
            
    def extractDoc(self, dataFetchResponse: Dict[str, Any]) -> Generator[Dict[str, Any]]:
        """
        This function extracts the relevant documents from the Elasticsearch response data.

        Parameters
        ----------
        dataFetchResponse : dict
            A dictionary containing the search results from Elasticsearch.

        Yields
        ------
        dict
            A dictionary containing the extracted documents.
        """
        entityKeys = self.parameters['types'].keys()
        hits = dataFetchResponse['hits']['hits']
        for hit in hits:
            data = (
                {
                    entityKey: dict(doc[entityKey])
                    for entityKey in entityKeys
                    for doc in hit.get('_source', [])
                }
            )
            yield data
            
    def parseDoc(self, doc):
        """
        This function deletes elements from a document whose score falls below a threshold defined by the user.

        Parameters
        ----------
        doc : dict
            A dictionary containing a document to be parsed.

        Yields
        ------
        dict
            A dictionary containing the parsed document.
        """
        for key, threshold in self.parameters["thresholds"].items():
            doc[key] = [d for d in doc[key] if d.get('score', 0) >= threshold]
        yield doc

    def docGenerator(self, dataFetchResponse):
        """
        This function generates a parsed document from the Elasticsearch response data.

        Parameters
        ----------
        dataFetchResponse : dict
            A dictionary containing the search results from Elasticsearch.

        Yields
        ------
        dict
            A dictionary containing the parsed document.
        """
        for doc in self.extractDoc(dataFetchResponse):
            doc = self.parseDoc(doc)
            yield doc
            
    def startProcess(self, queryCloudEvent):
        """
        This method is a runner function 

        Parameters
        ----------
        queryCloudEvent: dict
            This cloudevent has taxonomy details required to prepare a search Query to fetch data 

        Return 
        ------
        data: dict
            List of source entity and destination entity relationships             
        """

        dataFetchResponse = \
            ElasticSearchHandler(
                hosts=os.getenv('ES_HOSTS'), 
                username=os.getenv('ES_USERNAME'), 
                password=os.getenv('ES_PASSWORD'), 
                ca_certs=os.getenv('ES_CA_CERTS'), 
                ca_fingerprint=os.getenv('ES_CA_FINGERPRINT'), 
                index=os.getenv('ES_INDEX'),
                logger=self.logger,
            ).dataFetch(
                query=self.elasticsearchQueryBuilder(queryCloudEvent)
            )
        dataPushResponse = \
            Neo4jHandler(
                host=os.getenv('NEO4J_HOST'),
                user=os.getenv('NEO4J_USER'),
                password=os.getenv('NEO4J_PASSWORD'),
                neo4jParameters={'nodeTypes': [NodeType(nodeType).schema() for nodeType in self.parameters.get('types', []).values()],
                                 'chunkSize':10000}
            ).dataPush(
                queryParams=self.neo4jQueryBuilder(dataFetchResponse)
            )

        return dataPushResponse
    

