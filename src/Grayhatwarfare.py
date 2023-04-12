from nodeType import NodeType
from elasticsearch import Elasticsearch
import json

import sys
sys.path.insert(1, '../../sdk/data-ingress-common/')
sys.path.insert(1, '../../sdk/')

from dataFetcher import DataFetcher
from Neo4jHandler import Neo4jHandler
from ElasticSearchHandler import ElasticSearchHandler

from utils import loggerFunction
from util.neo4jConnection import Neo4jConnection, GraphDatabase
from util.openmetadataApi import OpenMetadataConnection, retrieveCatalogEntity  
from package.nodeType import NodeType

class GrayHatWarfare(DataFetcher):
    def __init__(self) -> None:
        super().__init__()
        self.neo4j = Neo4jConnection()
        self.openMeta = OpenMetadataConnection()
    
        # data used by both Elasticsearch and Neo4j
        self.from_node_keys = ['vendor'] 
        self.to_node_keys = ['relatedPersons', 'relatedOrganizations']
        self.relationshp_property_keys = ['amount']
        self.logger = loggerFunction()

        valid_node_types=['Person', 'Organization', 'Thing']
        self.neo4jClient = Neo4jHandler(
            valid_node_types=valid_node_types, 
            host='bolt://neo4j:7687', 
            user='neo4j', 
            password='test', 
            logger=self.logger
        )
        
    # Use retrieveCatalogEntity to check if the node exists or not in Neo4j
    # E.g. source_entity = {'name': "Person or Company name", "other fields from cloudEvent like email or location (if any)"}
    # E.g. For person use nodeType = NodeType.PERSON.value and use company use nodeType = NodeType.ORGANIZATION.value
    # E.g.  source_entity = retrieveCatalogEntity(openmeta_conn, source_entity, nodeType.lower())

    def dataFormatter(data,
                      vendor_threshold,
                      related_persons_threshold,
                      related_organizations_threshold,
                      amount_threshold,
                      entity_keys,
                      threshold_keys
                      ):
        
        # Cast string representation of dictionary to dictionary
        for entity_key in entity_keys:
                for idx in range(0, len(data[entity_key])):
                    data[entity_key][idx] = dict(data[entity_key][idx])

        # Delete any elements whose score falls below threshold defined by user
        thresholds = [vendor_threshold, related_persons_threshold, related_organizations_threshold, amount_threshold]
        for threshold_key, threshold in zip(threshold_keys, thresholds):
            data[threshold_key] = [d for d in data[threshold_key] if d.get('score', 0) >= threshold]

        return data
                
    def dataUpdate():
        return NotImplementedError


    def startProcess(self, elastic_cred, queryCloudEvent):
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
        # check keys for list objects
        def get_list_fields(data_dict: dict):
            if data_dict is None:
                data_dict = {}
            key_list = []
            for key, value in data_dict.items():
                # if list with 1+ dicts
                if isinstance(value, list) and len(value):
                    key_list.append(key)
            return key_list
        
        def recursive_loop(data_dict=None, entity_dict=None, key_list=None):
            if entity_dict is None:
                entity_dict = {}
            try:
                if not len(key_list):
                    neo4jQueriesArr.append(self.createNeo4jNodes(
                                        initial_node = entity_dict['vendor'], 
                                        terimal_node = entity_dict['entity'], 
                                        # optional arg; currency is assumed USD
                                        amount = data_dict['invoiceAmount'].get('answer', None)))
                else:
                        # Recursive case: iterate through the list of dicts associated with the current key
                        key = key_list.pop()
                        for sub_dict in data_dict.get(key, []):
                            entity_dict[key] = sub_dict['answer']
                            recursive_loop( 
                                        key_list = key_list, 
                                        entity_dict = entity_dict
                                        )                            
            # string_list is None
            except (TypeError, AttributeError) as e:
                print(e)
            # entity missing by key name in entity_dict. Currently 'vendor' or 'entity'
            except KeyError as e:
                print(e)

        neo4jQueriesArr = []

        subjectFields = {
            'person': ['vendor', 'relatedPersons'],
            'organization': ['relatedOrganizations']
        }
        selectedProperty = 'name'

        dataFetchResponse = \
            ElasticSearchHandler(
                hosts='', 
                username='', 
                password='', 
                ca_certs='', 
                ca_fingerprint='', 
                index='',
                logger=self.logger
            ).createElasticSearchPipeline(
                queryCloudEvent=queryCloudEvent, 
                subjectFields=subjectFields,
                selectedProperty=selectedProperty
            )
            for hit in dataFetchResponse['hits']['hits']:
                fmt_data = self.dataFormatter(data=hit['_source'],
                                            vendor_threshold=vendor_threshold,
                                            related_persons_threshold=related_persons_threshold,
                                            related_organizations_threshold=related_organizations_threshold,
                                            amount_threshold=amount_threshold,
                                            entity_keys = self.from_node_keys + self.to_node_keys,
                                            threshold_keys= self.from_node_keys + self.to_node_keys + self.relationshp_property_keys
                                            )
####################################################################################################################################
                key_list = get_list_fields(data_dict=fmt_data)
                recursive_loop(data_dict = fmt_data, 
                               key_list = key_list)
        neo4jQueries = ','.join(neo4jQueriesArr)
        neo4jQuery = f'MERGE {neo4jQueries}'
        dataPushResponse = self.dataPush(neo4jQuery)

# queryCloudEvent = {}
# queryCloudEvent['searchQueries'] = [{'key': 'name', 
#                         'value': 'person', 
#                         'subject': 'person', 
#                         'taxonomyNodeId': 'na35964166a184bd2b7a00c1b7c18f341',
#                         'properties': [{'key': 'startsWith', 'value': 'John SMith', 'subject': 'name', 'type': 'property', 'additionalProperties': {}}, 
#                                        {'key': 'startsWith', 'value': 'sales@google.com', 'subject': 'email', 'type': 'property', 'additionalProperties': {}}, 
#                                        {'key': 'startsWith', 'value': 'Los ANgeles, US', 'subject': 'address', 'type': 'property', 'additionalProperties': {}}],
#                         'additionalProperties': {}}]
# ingress = GrayHatWarfare()
# ingress.startProcess(queryCloudEvent=queryCloudEvent)