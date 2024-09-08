import os
from nodeTypes import NodeType
from Neo4jHandler import Neo4jHandler
from ElasticsearchHandler import ElasticsearchHandler
from typing import List, Dict, Generator
from multiprocessing import Pool
import logging

logger = logging.getLogger(__name__)


class ElasticsearchToNeo4jSync():
    def __init__(self) -> None:
        """
        Constructor method for the class. Change the values of parameters and neo4jParameters as necessary per ingress container

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        self.params: Dict[str, any] = {
            "properties": ['name'],
            "parse": {
                "thresholds": {
                    'args': {
                        'vendor': 0.9,
                        'relatedPersons': 0.9,
                        'relatedOrganizations': 0.9,
                        'amount': 0.9           
                    },
                    'condition': lambda threshold, entity: entity.get('score', 0) >= threshold
                },
            },
        }

        self.neo4jParams: Dict[str, any] = {
            "from": ["vendor"],
            "fromProps": ['answer'],
            "to": ['relatedPersons', 'relatedOrganizations'], 
            "toProps": ['answer'], 
            "relationship": ["HAS_PROVIDED_BUSINESS_TO"], 
            "relationshipProps": ['amount'],
            "propMap": {"answer": "name"},
            "types": {
                'vendor': 'person', 
                'relatedPersons': 'person', 
                'relatedOrganizations': 'organization', 
            },
        }
        logging.basicConfig(level=logging.INFO)
        self.neo4jParams = self.processNeo4jParams(neo4jParams=self.neo4jParams)
    
    def processNeo4jParams(self, neo4jParams):
        parsedNeo4jParams = self.equalizeListValues(data=neo4jParams)
        return parsedNeo4jParams
    
    def equalizeListValues(self, data):
        longest_key = max(data, key=lambda k: len(data[k]))
        longest_length = len(data[longest_key])

        for key in data:
            if not isinstance(data[key], list):
                logger.error(f"{key} value is not a list")
                continue
            if len(data[key]) < longest_length:
                data[key].extend([data[key][0]] * (longest_length - len(data[key])))
        return data

            
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
        properties: List[str] = self.params["properties"]
        typeProperties: List[str] = list(set(self.params.get('types', {}).values()))
        searchProperties: List[Dict[str, any]] = [eventSearchQuery.get('properties', []) for eventSearchQuery in queryCloudEvent.get('searchQueries', [])]
        try:
            queries = [{
                "multi_match": {
                    "query": searchProperty.get('value').lower(),
                    "fields": typeProperties,
                    "operator": "and",
                    "fuzziness": "AUTO"
                }
            } for searchProperty in searchProperties
            if searchProperty.get('subject', '') in properties]
            # Combine the must queries with a bool query
            searchQuery = {"bool": {"must": queries}} if queries else {}
        except Exception as e:
            logger.error(f"An error occurred in elasticQueryBuilder function: {str(e)}", exc_info=True)
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
        docs = self.generateDocuments(dataFetchResponse)
        fromTypeKeys = self.neo4jParams.get('from', [])
        toTypeKeys = self.neo4jParams.get('to', [])

        graphDataGen = ({
            "fromTypeKeys": self.neo4jParams.get('fromType', [])[idx],
            "fromPropsKeys": self.neo4jParams.get('fromProps', [])[idx],
            "toTypeKeys": self.neo4jParams.get('toType', [])[idx],
            "toPropsKeys": self.neo4jParams.get('toProps', [])[idx],
            "relationshipType": self.neo4jParams.get('relationship', [])[idx],
            "relationshipProps": self.neo4jParams.get('relationshipProps', [])[idx],
            "neo4jPropConvert" : self.neo4jParams.get("propMap", {}),
            "types": self.neo4jParams.get('types', {})
        } for idx in range(0, len(fromTypeKeys)))

        try:
            for doc in docs:
                yield self.buildGraphData(doc=doc, **next(graphDataGen))
        except Exception as e:
            logger.error(f"'An error occurred in neo4jQueryBuilder function: {str(e)}'", exc_info=True)

    def buildGraphData(self, from_type_key, to_type_key, relationship_type, from_props_keys, to_props_keys, relationship_props, doc, neo4jPropConvert, types):
        """
        This function builds the data required to create nodes and edges in Neo4j database.

        Parameters
        ----------
        from_type_key : str
            The key to the "from" node type in the neo4j parameters.
        to_type_key : str
            The key to the "to" node type in the neo4j parameters.
        relationship_type : str
            The type of the relationship in the neo4j parameters.
        from_props_keys : list
            A list of the keys for the "from" node properties in the neo4j parameters.
        to_props_keys : list
            A list of the keys for the "to" node properties in the neo4j parameters.
        relationship_props : list
            A list of the keys for the relationship properties in the neo4j parameters.
        doc : dict
            The document containing the data for the nodes and edges.
        neo4jPropConvert : dict
            A dictionary mapping property keys from Elasticsearch to Neo4j.
        types : dict
            A dictionary mapping node keys to node types.

        Returns
        -------
        dict
            A dictionary containing the data required to create nodes and edges in Neo4j database.
        """

        return {
            # Node Types
            'fromType': self.getType(types, from_type_key),
            'toType': self.getType(types, to_type_key),
            'edgeType': self.getType(types, relationship_type),
            # Node Properties
            'fromProps': self.getProps(from_props_keys, doc, neo4jPropConvert),
            'toProps': self.getProps(to_props_keys, doc, neo4jPropConvert),
            'edgeProps': self.getProps(relationship_props, doc, neo4jPropConvert)
        }

    def getProps(self, props: List[str], doc: Dict[str, Any], neo4jPropConvert: Dict[str, str]) -> Dict[str, Any]:
        """
        Returns a dictionary containing property keys and values for a given document.

        Parameters
        ----------
        props : list
            A list of property keys to extract from the document.
        doc : dict
            A dictionary containing document data.
        neo4jPropConvert : dict
            A dictionary containing mapping of Elasticsearch property names to Neo4j property names.

        Returns
        -------
        dict
            A dictionary containing property keys and values for a given document.
        """
        return {neo4jPropConvert[prop_key]: doc[prop_key] for prop_key in props}

    def getType(self, types: Dict[str, str], node: str) -> str:
        """
        Returns the type of node or relationship.

        Parameters
        ----------
        types : dict
            A dictionary containing the types of nodes and relationships.
        node : str
            The type of node or relationship.

        Returns
        -------
        str
            The type of node or relationship.
        """
        if node == 'relationship':
            types = self.neo4jParams
        return types.get(node, '')

    def extractDocument(self, dataFetchResponse: Dict[str, Any]) -> Generator[Dict[str, Any]]:
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
            
    def processDocument(self, doc):
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
        for parseKey, parseVal in self.parameters.get('parse', {}).items():
            parseArgsDict = parseVal[parseKey]['args']
            parseCondition = parseVal[parseKey]['condition']
            for argKey, argValue in parseArgsDict.items():
                doc[argKey] = [d for d in doc[argKey] if parseCondition(argValue, d)]
        yield doc 

    def generateDocumentsParallel(self, dataFetchResponse):
        """
        This function generates parsed documents using multiprocessing.

        Parameters
        ----------
        dataFetchResponse : dict
            A dictionary containing the search results from Elasticsearch.

        Yields
        ------
        dict
            A dictionary containing the parsed document.
        """
        with Pool(processes=self.n_jobs) as pool:
            for result in pool.imap_unordered(self.processDocument, self.extractDocument(dataFetchResponse)):
                if result is not None:
                    yield result

    def generateDocuments(self):
        """
        This function generates a parsed document.

        Yields
        ------
        dict
            A dictionary containing the parsed document.
        """
        docs = self.extractDocument()
        for parsed_doc in map(self.processDocument, docs):
            yield parsed_doc
            
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
            ElasticsearchHandler(
                hosts=os.getenv('ES_HOSTS'), 
                username=os.getenv('ES_USERNAME'), 
                password=os.getenv('ES_PASSWORD'), 
                ca_certs=os.getenv('ES_CA_CERTS'), 
                ca_fingerprint=os.getenv('ES_CA_FINGERPRINT'), 
                index=os.getenv('ES_INDEX'),
                logger=logger,
            ).dataFetch(
                query=self.elasticsearchQueryBuilder(queryCloudEvent)
            )
        dataPushResponse = \
            Neo4jHandler(
                host=os.getenv('NEO4J_HOST'),
                user=os.getenv('NEO4J_USER'),
                password=os.getenv('NEO4J_PASSWORD'),
                neo4jParameters={'nodeTypes': [NodeType(nodeType).schema() for nodeType in self.params.get('types', []).values()],
                                 'chunkSize':10000,
                                 'reqProps': self.params['properties']}
            ).dataPush(
                queryParams=self.neo4jQueryBuilder(dataFetchResponse)
            )

        return dataPushResponse
