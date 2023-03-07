import os
import sys
import logging
import datetime
import requests
from requests.exceptions import HTTPError

from util.openmetadataApi import OpenMetadataConnection 

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

SEARCHRESPONSE_TEMPLATE= {
    "status":"unknown",
    "queryType":"thing",
    "queryDepth":1,
    "correlationId":"dbc33a1c75fd4040b3a241cc495ec868",
    "labels": ["Frederick"],
    "filters": [
        {
        "value": "Calk",
        "key": "familyName"
        },
        {
        "value": "Frederick",
        "key": "givenName"
        },
        {
        "value": "Atlanta, GA",
        "key": "birthPlace"
        }
    ],
    "nodes":[
        {"popularityScore": 10.0, 
         "address": "123 main street", 
         "tag_suggest": None, 
         "worksFor": "US, LLC", 
         "entityType": "person", 
         "givenName": "Frederick", 
         "suggest": ["123 main street", "UCLA", "test@gmail.com", "Frederick", "Freddy"], 
         "schemaName":"/person", "birthDate":"12-25-1998", 
         "tags":None, 
         "birthPlace":"Atlanta, GA", 
         "detailedDescription":"string", 
         "affiliation":"bacon high", 
         "familyName":"Calk", 
         "alumniOf":"UCLA", 
         "name":"Freddy", 
         "categories":"person, thing", 
         "id":"bb7b17cd-20ca-46d1-9987-0a035b6795ef", 
         "additionalName":"Fred", 
         "email":"test@gmail.com"}, 
        {"popularityScore": 10.0, 
         "address": "123 main street", 
         "tag_suggest": None, 
         "worksFor": "US, LLC", 
         "entityType": "person", 
         "givenName": "Frederick", 
         "suggest": ["123 main street", "UCLA", "test@gmail.com", "Frederick", "Freddy"], 
         "schemaName":"/person", "birthDate":"12-25-1998", 
         "tags":None, 
         "birthPlace":"Atlanta, GA", 
         "detailedDescription":"string", 
         "affiliation":"bacon high", 
         "familyName":"Calk", 
         "alumniOf":"UCLA", 
         "name":"Freddy", 
         "categories":"person, thing", 
         "id":"18491849-be8a-46c8-9709-a2467785c7fb", 
         "additionalName":"Fred", 
         "email":"test@gmail.com"
        }
    ]
}



TEMPLATE2 = {
    "queryType": "person", 
    "correlationId": "773cc50b-da83-48a3-88b5-731bafec83d4", 
    "labels": ["additionalName", "additionalType", "address", "affiliation", "alternateName", "alumniOf", "award", "awards", "birthDate", "birthPlace", "brand", "children", "colleague", "colleagues", "contactPoint", "contactPoints", "deathDate", "deathPlace", "description", "duns", "email", "familyName", "faxNumber", "follows", "gender", "givenName", "globalLocationNumber", "hasOfferCatalog", "hasPOS", "height", "homeLocation", "honorificPrefix", "honorificSuffix", "image", "isicV4", "jobTitle", "knows", "mainEntityOfPage", "makesOffer", "memberOf", "naics", "name", "nationality", "netWorth", "owns", "parent", "parents", "performerIn", "potentialAction", "relatedTo", "sameAs", "seeks", "sibling", "siblings", "spouse", "taxID", "telephone", "url", "vatID", "weight", "workLocation", "worksFor", "detailedDescription", "popularityScore"], "filters": [{"value": "Calk", "key": "familyName"}, {"value": "Frederick", "key": "givenName"}, {"value": "Atlanta, GA", "key": "birthPlace"}], 
    "nodes": [
        {"popularityScore": 10.0, "address": "123 main street", "tag_suggest": None, "worksFor": "US, LLC", "entityType": "person", "givenName": "Frederick", "suggest": ["123 main street", "UCLA", "test@gmail.com", "Frederick", "Freddy"], "schemaName":"/person", "birthDate":"12-25-1998", "tags":None, "birthPlace":"Atlanta, GA", "detailedDescription":"string", "affiliation":"bacon high", "familyName":"Calk", "alumniOf":"UCLA", "name":"Freddy", "categories":"person, thing", "id":"bb7b17cd-20ca-46d1-9987-0a035b6795ef", "additionalName":"Fred", "email":"test@gmail.com"}, {"popularityScore": 10.0, "address": "123 main street", "tag_suggest": None, "worksFor": "US, LLC", "entityType": "person", "givenName": "Frederick", "suggest": ["123 main street", "UCLA", "test@gmail.com", "Frederick", "Freddy"], "schemaName":"/person", "birthDate":"12-25-1998", "tags":None, "birthPlace":"Atlanta, GA", "detailedDescription":"string", "affiliation":"bacon high", "familyName":"Calk", "alumniOf":"UCLA", "name":"Freddy", "categories":"person, thing", "id":"18491849-be8a-46c8-9709-a2467785c7fb", "additionalName":"Fred", "email":"test@gmail.com"}
        ]
    }

class ResultAggregator():
    def __init__(self) -> None:
        self.openmetadata_conn = OpenMetadataConnection()
    
    # def aggregate(self, resultset):
    #     nodes = []
    #     edges = []
    #     nodes_dict ={}
    #     for r in resultset:
    #         for k ,v in r.items():
    #             if type(v) == dict: #is Node
    #                 node_id = v['id']
    #                 if node_id not in nodes_dict:
    #                     nodes_dict[node_id] = {}
    #                     nodes_dict[node_id]['id'] = node_id
    #                     nodes_dict[node_id]['name'] = v['text'] if 'text' in v else v['name']
    #                     nodes_dict[node_id]['entityType'] = v['NodeType'] if 'NodeType' in v else "Thing" # TODO get nodetype
    #                     nodes_dict[node_id]['externalId'] = v['id']
    #                     nodes_dict[node_id]['alternateName'] = ""
    #                     nodes_dict[node_id]['additionalType'] = set()
    #                     nodes_dict[node_id]['additionalType'].add('thing')
    #                     nodes_dict[node_id]['additionalType'].add(nodes_dict[node_id]['entityType'].lower())
    #                     nodes_dict[node_id]['additionalType'] = list(nodes_dict[node_id]['additionalType'])
    #                     nodes_dict[node_id]['description'] = ""
    #                     nodes_dict[node_id]['mainEntityOfPage'] = ""
    #                     nodes_dict[node_id]['potentialAction'] = ""
    #                     nodes_dict[node_id]['url'] = ""
    #                     nodes_dict[node_id]['sameAs'] = ""
    #                     nodes_dict[node_id]['detailedDescription'] = ""
    #                     nodes_dict[node_id]['popularityScore'] = 0.1
                        
    #                     nodes.append(nodes_dict[node_id])
    #             elif type(v) == tuple: # is link
    #                 node_id = v[0]['id']
    #                 node = nodes_dict[node_id]
    #                 node[v[1]] = v[2]['text']
    #                 edges.append(
    #                     { 'relationship':v[1],
    #                      'sourceId':v[0]['id'],
    #                      'destinationId':v[2]['id'],
    #                      }
    #                 )
    #     return nodes, edges
    def aggregate(self, resultset):
        nodes = []
        edges = []
        nodes_dict ={}
        for r in resultset:
            try:
                for node in r['nodes']:
                    node_id = node['id']
                    if node_id not in nodes_dict:
                        nodes_dict[node_id] = {}
                        nodes_dict[node_id]['id'] = node_id
                        nodes_dict[node_id]['name'] = node['text'] if 'text' in node else node['name'] if 'name' in node else 'NO_NAME'
                        nodes_dict[node_id]['entityType'] = node['NodeType'] if 'NodeType' in node else "Thing" # TODO get nodetype
                        nodes_dict[node_id]['externalId'] = node['id']
                        nodes_dict[node_id]['alternateName'] = ""
                        nodes_dict[node_id]['additionalType'] = set()
                        nodes_dict[node_id]['additionalType'].add('thing')
                        nodes_dict[node_id]['additionalType'].add(nodes_dict[node_id]['entityType'].lower())
                        nodes_dict[node_id]['additionalType'] = list(nodes_dict[node_id]['additionalType'])
                        nodes_dict[node_id]['description'] = ""
                        nodes_dict[node_id]['mainEntityOfPage'] = ""
                        nodes_dict[node_id]['potentialAction'] = ""
                        nodes_dict[node_id]['url'] = node['url'] if 'url' in node else ""
                        nodes_dict[node_id]['sameAs'] = ""
                        nodes_dict[node_id]['detailedDescription'] = ""
                        nodes_dict[node_id]['popularityScore'] = 0.1
                        nodes.append(nodes_dict[node_id])
                for edge in r['relationships']:
                    #node_id = v[0]['id']
                    #node = nodes_dict[node_id]
                    #node[v[1]] = v[2]['text']
                    edges.append(
                            { 'relationship':edge[1],
                            'sourceId':edge[0]['id'],
                            'destinationId':edge[2]['id'],
                            }
                        )
            except Exception as err:
                logger.exception(f'Unable to parse NEO4J resultset: {err}')
        return nodes, edges
    
    def persist(self, nodes, edges, message=None):
        
        #if message != None:
        #    data = message
        #else:
        data = SEARCHRESPONSE_TEMPLATE
        data['nodes'] = nodes
        data['edges'] = edges
        
        if message != None:
            if 'correlationId' in message:
                data['correlationId'] = message['correlationId']
            if 'queryType' in message:
                data['queryType'] = message['queryType']
            else:
                data['queryType'] = 'thing'
                
            if 'labels' in message:
                data['labels'] = message['labels']
            else:
                data['labels'] = []
                
            if 'queryDepth' in message:
                data['queryDepth'] = message['queryDepth']
            else:
                data['queryDepth'] = 0
                
            if 'filters' in message:
                data['filters'] = message['filters']
            else:
                data['filters'] = []
                
            if 'status' in message:
                data['status'] = message['status']
            else:
                data['status'] = "unknown"
        
        response = self.openmetadata_conn.put('/v1/search/result',data)
        if response != None:
            logger.info(f'Search query saved: {data}')
        else:
            logger.error(f'The error occurred in message: {message}')