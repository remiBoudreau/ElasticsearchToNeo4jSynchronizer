from .nodeType import NodeType
import logging
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk


import sys, uuid, re
sys.path.insert(1, '../../sdk/data-ingress-common/')
sys.path.insert(1, '../../sdk/')
from googleapiclient.discovery import build
from dataFetcher import DataFetcher
from utils import loggerFunction
from util.neo4jConnection import Neo4jConnection, GraphDatabase
from util.openmetadataApi import OpenMetadataConnection, retrieveCatalogEntity  
from package.nodeType import NodeType
from datetime import datetime
from decouple import config
from names_matcher import NamesMatcher


# Logger 
logger = loggerFunction()
  
class GrayHatWarfare(DataFetcher):
    def __init__(self) -> None:
        super().__init__()
        self.neo4j = Neo4jConnection()
        self.openMeta = OpenMetadataConnection()
    
    # Use retrieveCatalogEntity to check if the node exists or not in Neo4j
    # E.g. source_entity = {'name': "Person or Company name", "other fields from cloudEvent like email or location (if any)"}
    # E.g. For person use nodeType = NodeType.PERSON.value and use company use nodeType = NodeType.ORGANIZATION.value
    # E.g.  source_entity = retrieveCatalogEntity(openmeta_conn, source_entity, nodeType.lower())

    def run_query(self,query, params={}):
        """
        This method connects to the Neo4j database
        
        Parameters
        ----------
        query : 
            Query to run on Neo4j 
        
        params: dict
            Parameters used to dump Neo4j

        Return 
        ------
        result
            Return response from Neo4j
        """
        host = 'bolt://neo4j:7687'
        user = 'neo4j'
        password = 'test'
        driver = GraphDatabase.driver(host,auth=(user, password))
        try:
            with driver.session() as session:
                result = session.run(query, params)
                return result
        except Exception as e:
            logger.warn(f"Couldn't parse text due to {e}")
            pass

    def createNeo4jNodes(self, dataFetchResponse):
        """
        Descript:
        
        Parameters
        ----------
        dataFetchResponse : dict
            Descript
            
        Return 
        ------
        : int
            Returns 0 if relationships were created/found else 1
        """
        # print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$', dataFetchResponse)
        
        # id_person = str(dataFetchResponse["id"])

        # # if not id_person:
        # id_person_query = '''
        # MERGE (p:Person {
        #     id: "%s",
        #     name: "%s",
        #     nodeType: "%s",
        #     url: "%s"})
        # RETURN p.id
        # ''' % (
        #     str(id_person),
        #     str(dataFetchResponse['name']),
        #     NodeType.PERSON.value,
        #     ''
        # )
        # print(id_person_query)
        # _ = self.run_query(query=id_person_query) 
        # # id_person = str(dataFetchResponse["id"])
                    
        # red_flag_node_query = '''
        # MERGE (f:criminal_conduct_red_flag {
        #     flag_reason: "%s", 
        #     nameLast: "%s", 
        #     nameFirst: "%s", 
        #     url: "%s",
        #     id: "%s" })
        # RETURN f.id
        # ''' % (
        #     str(dataFetchResponse['flag_reason']), 
        #     str(dataFetchResponse['lastName']), 
        #     str(dataFetchResponse['firstName']), 
        #     str(dataFetchResponse['url']),                  #TODO: Add incarnation state to this node properties
        #     str(uuid.uuid4())
        # )
        # id_flag = self.neo4j.query(query=red_flag_node_query)

        # person_red_flag_edge = '''
        # MATCH (p:Person {id: "%s"}), (f:criminal_conduct_red_flag {id: "%s"})
        # CREATE (p)-[:has_criminal_conduct_red_flag]->(f)
        # ''' % (id_person, id_flag[0]['f.id'])
        # print("personNeo4j",person_red_flag_edge)
        # node_in_neo4j = self.neo4j.query(query=person_red_flag_edge)
        # return node_in_neo4j
    
    def dataPush(self, formatedData):
        """
        Function to dump your neo4j nodes and relationships to neo4j 
        Note: Do not delete this function if you are not using. """
        return NotImplementedError

    def queryBuilder(self, queryCloudEvent):
        """
        This function extracts the person name or company name that you need for search. Feel free to change the return data as per your needs.
        
        Parameters
        ----------
        queryCloudEvent : dict
            This cloudevent has taxonomy details required to prepare a search Query to fetch data from ES index
            
        Return 
        ------
        searchQueryContentDict: dict or None
            Dict contains the search query parameters
        """
        searchQuery = {
            "query": {
                "bool": {
                    "should": []
                }
            }
        }
        for document in queryCloudEvent['searchQueries']:
            for key, value in document.items():
            # if person
                searchQuery['bool']['should'].extend([{
                                    "match": {
                                        "vendor": {
                                            "query": "<user search string>",
                                            "operator": "and"
                                        }
                                    }
                                },
                                {
                                    "nested": {
                                        "path": "relatedPersons",
                                        "query": {
                                            "match": {
                                                "relatedPersons": {
                                                    "query": "<user search string>",
                                                    "operator": "and"
                                                }
                                            }
                                        }
                                    }
                                }]
            )
            # if organization
                searchQuery.append({
                                "nested": {
                                    "path": "relatedOrganizations",
                                    "query": {
                                        "bool": {
                                            "must": [
                                                {
                                                    "match": {
                                                        "relatedOrganizations": {
                                                            "query": "<user search string>",
                                                            "operator": "and"
                                                        }
                                                    }
                                                }
                                            ]
                                        }
                                    }
                                }
                            })
        # Define the search query
        return searchQuery
    
    def dataFetch(self, search_query):
        """
        This function with take the searchQuery you generated in  queryBuilder and get the data from your datasource
        
        Parameters
        ----------
        searchQueryContentDict: dict
            Dict contains the search query parameters

        Return 
        ------
        dataFetchResponse: dict 
            Dict containing first completely matched candidate (name and location). 
            If not found provides a partially matched candidate (name only).
            If no partially matched candidates, returns None
        """  
        client = Elasticsearch(
            hosts='https://localhost:9200/',
            # ssl_assert_fingerprint=str(ca_fingerprint),
            # ca_certs='http_ca.crt',
            basic_auth=('elastic', '8+fG8-l1A_zgSp1HTHAQ'),
            verify_certs=False
        )
        results = client.search(index="grayhatwarfare", body=search_query)
        return results

    def dataFormatter():
        """This is helper functiion for fancy Neo4j query. Please ignore. """  
        return NotImplementedError

    def dataUpdate():
        return NotImplementedError


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
        searchQuery = self.queryBuilder(queryCloudEvent)
        dataFetchResponse = self.dataFetch(searchQuery)
        for hit in dataFetchResponse['hits']['hits']:
            self.createNeo4jNodes(hit['_source'])
            
import os

INDEX = os.getenv("ES_INDEX")
HOST = os.getenv("ES_HOST")
CA_CERTS = os.getenv("ES_CA_CERTS")
CA_FINGERPRINT = os.getenv("ES_FINGERPRINT")
USERNAME = os.getenv("ES_USERNAME")
PASSWORD = os.getenv("ES_PASSWORD")