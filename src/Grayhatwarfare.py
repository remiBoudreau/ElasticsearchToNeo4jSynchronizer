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

from typing import List, Dict
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
        # self.neo4jClient = Neo4jHandler(
        #     valid_node_types=valid_node_types, 
        #     host='bolt://neo4j:7687', 
        #     user='neo4j', 
        #     password='test', 
        #     logger=self.logger
        # )

    # Use retrieveCatalogEntity to check if the node exists or not in Neo4j
    # E.g. source_entity = {'name': "Person or Company name", "other fields from cloudEvent like email or location (if any)"}
    # E.g. For person use nodeType = NodeType.PERSON.value and use company use nodeType = NodeType.ORGANIZATION.value
    # E.g.  source_entity = retrieveCatalogEntity(openmeta_conn, source_entity, nodeType.lower())    
    # 
    def KeyErrorHandler(self, key, obj_type=None):
        # default type to list
        if obj_type is None:
            obj_type = list
        self.logger.error(f"KeyError: '{key}' is not present in the given object")
        return obj_type()
    
    def dataFormatter(self,
                    dataFetchResponse,
                    vendor_threshold,
                    related_persons_threshold,
                    related_organizations_threshold,
                    amount_threshold,
                    entity_keys,
                    threshold_keys,
                    keyDependencyGenerator
                    ):
        hits = dataFetchResponse.get(*next(keyDependencyGenerator))
        data = [
            {
                entity_key: [dict(item) for item in data.get(entity_key, self.KeyErrorHandler(entity_key, type(data.get(entity_key,))))] 
                for entity_key in entity_keys
                for data in [hit.get(*next(keyDependencyGenerator))]
            }
            for hit in hits.get(*next(keyDependencyGenerator))
        ]
        data = [{key: value for key, value in hit.items() if key in entity_keys} for hit in data]

        if not hits:
            self.logger.error("No hits returned from Elasticsearch")
        elif not data:
            self.logger.error("KeyError _source found in Elasticsearch response")

        # Delete any elements whose score falls below threshold defined by user
        thresholds = [vendor_threshold, related_persons_threshold, related_organizations_threshold, amount_threshold]
        for threshold_key, threshold in zip(threshold_keys, thresholds):
            data[threshold_key] = [d for d in data[threshold_key] if d.get('score', 0) >= threshold]
            
        return data

    # def dataFormatter(
    #         self,
    #         dataFetchResponse,
    #         vendor_threshold,
    #         related_persons_threshold,
    #         related_organizations_threshold,
    #         amount_threshold,
    #         entity_keys,
    #         threshold_keys,
    #         keyDependencyGenrator):
    #     hits = dataFetchResponse.get(*next(keyDependencyGenerator))
    #     data = [
    #         {
    #             entity_key: [dict(item) for item in data.get(entity_key, self.KeyErrorHandler(entity_key, type(data.get(entity_key,))))] 
    #             for entity_key in entity_keys
    #             for data in [hit.get(*next(keyDependencyGenerator))]
    #         }
    #         for hit in hits.get(*next(keyDependencyGenerator))
    #     ]
    #     data = [{key: value for key, value in hit.items() if key in entity_keys} for hit in data]

    #     if not hits:
    #         self.logger.error("No hits returned from Elasticsearch")
    #     elif not data:
    #         self.logger.error("KeyError _source found in Elasticsearch response")

    # # Delete any elements whose score falls below threshold defined by user
    # thresholds = [vendor_threshold, related_persons_threshold, related_organizations_threshold, amount_threshold]
    # for threshold_key, threshold in zip(threshold_keys, thresholds):
    #     data[threshold_key] = [d for d in data[threshold_key] if d.get('score', 0) >= threshold]
        
    # return data


    def dataFormatter(self,
                      dataFetchResponse,
                      vendor_threshold,
                      related_persons_threshold,
                      related_organizations_threshold,
                      amount_threshold,
                      entity_keys,
                      threshold_keys
                      ):
        keyDependencyGenerator = ((key[0], self.KeyErrorHandler(key[0], key[1])) for key in keyDependencyMap)
        hits = dataFetchResponse.get(*next(keyDependencyGenerator))
        for hit in hits.get(*next(keyDependencyGenerator)):
            data = hit.get(*next(keyDependencyGenerator))
            # Cast string representation of dictionary to dictionary
            for entity_key in entity_keys:
                for idx in range(0, len(data.get(entity_key, self.KeyErrorHandler(entity_key, type(data.get(entity_key,)))))):
                    data[entity_key][idx] = dict(data[entity_key][idx])
            data = {key: [dict(item) for item in value] for key, value in data.items() if key in entity_keys}

        if not hits:
            self.logger.error("No hits returned from Elasticsearch")
        elif not data:
            self.logger.error("KeyError _source found in Elasticsearch response")
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

        subjectFields = {
            'person': ['vendor', 'relatedPersons'],
            'organization': ['relatedOrganizations']
        }
        selectedProperty = 'name'

        dataFetchResponse = \
            ElasticSearchHandler(
                hosts=os.getenv('hosts'), 
                username=os.getenv('username'), 
                password=os.getenv('password'), 
                ca_certs=os.getenv('ca_certs'), 
                ca_fingerprint=os.getenv('ca_fingerprint'), 
                index=os.getenv('index'),
                logger=self.logger
            ).createElasticSearchPipeline(
                queryCloudEvent=queryCloudEvent, 
                subjectFields=subjectFields,
                selectedProperty=selectedProperty
            )
            
        dataFormatResponse = \
            self.dataFormatter(
                dataFetchResponse=dataFetchResponse,
                vendor_threshold=self.vendor_threshold,
                related_persons_threshold=self.related_persons_threshold,
                related_organizations_threshold=self.related_organizations_threshold,
                amount_threshold=self.amount_threshold,
                entity_keys = self.from_node_keys + self.to_node_keys,
                threshold_keys= self.from_node_keys + self.to_node_keys + self.relationshp_property_keys
            )
    
        dataPushResponse = \
            Neo4jHandler(
                    valid_node_types=subjectFields.keys(),
                ).createNeo4jPipeline(
                    dataFormatResponse=dataFormatResponse
                )