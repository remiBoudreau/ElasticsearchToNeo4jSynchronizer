from logging import Logger
from neo4j import GraphDatabase
from package.nodeType import NodeType

class Neo4jHandler(object):
    def __init__(self, valid_node_types, host, user, password, logger: Logger):
        self.valid_node_types = [NodeType(valid_node_type).schema() for valid_node_type in valid_node_types]
        self.driver = GraphDatabase.driver(host=host, auth=(user, password))
        self.neo4jQueriesArr = []
        self.logger = logger

    def createNode(self, node_type, node_name):
        if node_type.capitalize() not in self.valid_node_types:
            self.logger.error(f"Invalid node type detected. Must be one of {', '.join(self.valid_node_types)}")

        return f"(:{node_type.capitalize()} {{name: '{node_name}'}})"

    def createDyad(self, from_node_type, from_node_name, relationship_type, to_node_type, to_node_name, amount=None):
        from_node = self.create_node(from_node_type, from_node_name)
        to_node = self.create_node(to_node_type, to_node_name)

        amount_str = f"amount: {amount}" if amount is not None else ""
        return f"{from_node}-[:{relationship_type} {{{amount_str}}}]->{to_node}"

    def dataPush(self, query, params={}):
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
        try:
            with self.driver.session() as session:
                result = session.run(query, params)
                return result
        except Exception as e:
            self.logger.warn(f"Couldn't parse text due to {e}")
            pass

    def getListTypeValues(self, data_dict: dict):
        if data_dict is None:
            data_dict = {}
        key_list = []
        for key, value in data_dict.items():
            # if list with 1+ dicts
            if isinstance(value, list) and len(value):
                key_list.append(key)
        return key_list 

    def queryBuilder(self, data_dict=None, entity_dict=None, key_list=None, neo4jQueriesArr=None):
        if entity_dict is None:
            entity_dict = {}
        if neo4jQueriesArr is None:
            neo4jQueriesArr = []
        if key_list is None:
            key_list = self.getListTypeValues(data_dict=data_dict)
        
        try:
            if all(key in entity_dict for key in key_list):
                neo4jQueriesArr.append(
                    self.createDyad(
                        from_node_type='Vendor',
                        from_node_name=entity_dict['vendor'], 
                        relationship_type='HAS_PROVIDED_BUSINESS_TO', 
                        to_node_type='Entity', 
                        to_node_name=entity_dict['entity'], 
                        amount=data_dict['invoiceAmount'].get('answer', None)
                    )
                )
            else:
                # Recursive case: iterate through the list of dicts associated with the current key
                key = key_list[0]
                for sub_dict in data_dict.get(key, []):
                    entity_dict[key] = sub_dict['answer']
                    self.neo4jQueryBuilder( 
                                key_list = key_list[1:], 
                                entity_dict = entity_dict
                                )

        # string_list is None
        except (TypeError, AttributeError) as e:
            print(e)
        # entity missing by key name in entity_dict. Currently 'vendor' or 'entity'
        except KeyError as e:
            print(e)

    def parseNeo4jQuery(self, ):
        neo4jQueries = ','.join()
        neo4jQuery = f'MERGE {neo4jQueries}' 
        dataPushResponse = self.dataPush(neo4jQuery)
        return dataPushResponse

    def neo4jOperations(self, queryCloudEvent, subjectFields, selectedProperty, entityKeys):
        neo4jCypher = self.queryBuilder(queryCloudEvent, subjectFields, selectedProperty)                #1
        dataPushResponse = self.dataPush(neo_4j_cypher=neo4jCypher, entity_keys=entityKeys) #2
        return dataPushResponse
    
    def createNeo4jPipeline(self, data):
        return map(lambda searchQuery: self.neo4jOperations(), dataFormatResponse)
    
