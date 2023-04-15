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

from typing import List, Dict, Generator

class GrayHatWarfare(DataFetcher):
    def __init__(self) -> None:
        super().__init__()
        self.neo4j = Neo4jConnection()
        self.openMeta = OpenMetadataConnection()

        self.parameters = {"selectedProperties": ['name'],
                            "thresholds": {
                                'vendor': 0.9,
                                'relatedPersons': 0.9,
                                'relatedOrganizations': 0.9,
                                'amount': 0.9
                                },
                            "entityTypes": {
                                'vendor': 'person', 
                                'relatedPersons': 'person', 
                                'relatedOrganizations': 'organization', 
                                }

                            }
        self.neo4jParameters = {
                    "from": ["vendor"],
                    "fromProps": [],
                    "to": ['relatedPersons', 'relatedOrganizations'], 
                    "toProps": [], 
                    "relationship": "HAS_PROVIDED_BUSINESS_TO", 
                    "relationshipProps": ['amount']
                    }
        self.logger = loggerFunction()
            
    # Use retrieveCatalogEntity to check if the node exists or not in Neo4j
    # E.g. source_entity = {'name': "Person or Company name", "other fields from cloudEvent like email or location (if any)"}
    # E.g. For person use nodeType = NodeType.PERSON.value and use company use nodeType = NodeType.ORGANIZATION.value
    # E.g.  source_entity = retrieveCatalogEntity(openmeta_conn, source_entity, nodeType.lower())

    def elasticsearchQueryBuilder(self, queryCloudEvent):
        search_query = None
        try:
        # list of queries
            queries = [{
                "multi_match": {
                    "query": property['value'].lower(),
                    "fields": self.parameters['entityTypes'][event_search_query['subject']],
                    "operator": "and",
                    "fuzziness": "AUTO"
                }
            } for event_search_query in queryCloudEvent['searchQueries'] 
            for property in event_search_query['properties'] 
            if property['key'] in self.parameters["selectedProperties"]]
            # Combine the must queries with a bool query
            search_query = {"bool": {"must": queries}}
        except KeyError as e:
            self.logger.error(f'Failed to parse queryEventHandler: {e}')
        finally:
            return search_query
            
    def neo4jQueryBuilder(self, dataFetchResponse):
        for doc in self.docGenerator(dataFetchResponse):
            for nodeFrom in [entityKey.lower() for entityKey in self.neo4jParameters['from']]:
                for nodeTo in [entityKey.lower() for entityKey in self.neo4jParameters['to']]:
                        self.neo4jParameters['name'] = doc[nodeFrom]
                        yield {
                            'fromType': self.parameters['entityTypes'][nodeFrom],
                            'fromProps': {'name': doc[nodeFrom], **{key: doc[key] for key in self.neo4jParameters['fromProps'] if key != "name"}},
                            'toType': self.parameters['entityTypes'][nodeTo],
                            'toProps':{'name': doc[nodeTo], **{key: doc[key] for key in self.neo4jParameters['fromProps'] if key != "name"}},
                            'relationshipType': self.neo4jParameters['relationship'],
                            'relationshipProps': {key: doc[key] for key in self.neo4jParameters['relationshipProps']}
                        }

    def extractDoc(self, dataFetchResponse: dict) -> Generator[dict, None, None]:
        """
        Extracts data from an Elasticsearch search response.

        Parameters:
            dataFetchResponse (dict): The response object returned by Elasticsearch.

        Yields:
            dict: A dictionary containing the extracted data.
        """
        try:
            hits = dataFetchResponse.get('hits')
            for hit in hits.get('hits'):
                data = (
                    {
                        entityKey: dict(doc[entityKey])
                        for entityKey in self.parameters['entityTypes'].keys()
                        for doc in hit.get('_source')
                    }
                )
                yield data
        except Exception as e:
            self.logger.error(e)
    
    def parseDoc(self, doc):
        # Delete any elements whose score falls below threshold defined by user
        for key, threshold in self.parameters["thresholds"]:
            doc[key] = [d for d in doc[key] if d.get('score', 0) >= threshold]
        yield doc


    def docGenerator(self, dataFetchResponse):
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
            
            
        How will this class work???
        1.  startProcess will get the incoming queryCloudEvent (can be person or company cloudevent depending on your datasource)
        2.  queryBuilder function will extract the information from this cloudEvent (E.g. personName or companyName)
        3.  dataFetch function will take the searchString you got from queryBuilder and get the data from respective datasource
        4.  dataPush function will create/take the neo4j nodes and relationships and will dump to neo4j                
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
                neo4jParameters={'nodeTypes':list(set(value for value in self.parameters['entityTypes'].values())),
                                 'chunkSize':10000}
            ).dataPush(
                queryParams=self.neo4jQueryBuilder(dataFetchResponse)
            )

        return dataPushResponse
    

