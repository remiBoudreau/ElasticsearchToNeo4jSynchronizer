from logging import Logger
from neo4j import GraphDatabase
from contextlib import contextmanager
from neo4j import ResultSummary
from typing import List, Dict, Union
from nodeType import NodeType

class Neo4jHandler():
    def __init__(self, neo4jParameters: Dict, uri: str, user: str, password: str, logger: Logger) -> None:
        """
        Initializes a Neo4jHandler object.

        Parameters
        ----------
        neo4jParameters : dict
            A dictionary containing the Neo4j database parameters.
        host : str
            The hostname or IP address of the Neo4j database.
        user : str
            The username to use when connecting to the Neo4j database.
        password : str
            The password to use when connecting to the Neo4j database.
        logger : Logger
            A logger object used to log events and error messages.
        """
        self.params = neo4jParameters
        self.driver = GraphDatabase.driver(uri=uri, auth=(user, password))
        self.logger = logger
        self.validTypes = {_nodeType.schema() for _nodeType in NodeType}


    def formatProps(self, props: Dict) -> str:
        """
        Formats node or relationship properties as a string to be used in a Cypher query.

        Parameters
        ----------
        props : dict
            A dictionary containing the node or relationship properties.

        Returns
        -------
        props : str
            A string containing the formatted node or relationship properties.
        """
        if isinstance(props, dict) and props:
            propStr = ', '.join([f"{str(k)}: '{str(v)}'" for k, v in props.items()])
            return f"{{{propStr}}}"
        else:
            return ""
        
    def createRelationship(self, relationshipType: str, relationshipProps: Dict) -> str:
        """
        Creates a Cypher relationship string to be used in a Cypher query.

        Parameters
        ----------
        relationshipType : str
            The type of relationship to create.
        relationshipProps : dict
            A dictionary containing the properties to be set on the relationship.

        Returns
        -------
        rel_str : str
            A string containing the Cypher relationship.
        """
        relationship = self.formatProps(relationshipProps)
        if relationshipType and not relationship:
            self.createDyadErrorHandler(errorHandling='relationship')
            return ""

        return f"-[:{relationshipType} {{{relationship}}}]->"
    
    def createNode(self, _nodeType: str, _nodeProps: Dict) -> str:
        """
        Creates a Cypher node string to be used in a Cypher query.

        Parameters
        ----------
        _nodeType : str
            The type of node to create.
        _nodeProps : dict
            A dictionary containing the properties to be set on the node.

        Returns
        -------
        node : str
            A string containing the Cypher node.
        """
        node=''
        if not _nodeType or _nodeType not in self.validTypes:
            self.createDyadErrorHandler(errorType='nodeType', entityType=_nodeType)
            return node
        if any(map(__func=bool(),
                   __iter=[excludeMember for excludeMember in self.params.get('excludeMembers', [])])):
            self.createDyadErrorHandler(errorType='keyTypes', entityType='nodeTypes')
            return node
        else:
            propStr = self.formatProps(_nodeProps)
            node = f"(:{_nodeType} {propStr})"

        if _nodeType and not node:
            self.createDyadErrorHandler(errorMessages='node', entityType=_nodeType)

        return node

    def createDyadErrorHandler(self, errorType: str, entityType: str = 'undefined'):
        errorMessages = {
            "node": f"ValueError: failed to create node of type {entityType}.",
            "noNameProp": f"AttributeError: failed to find `name` in neo4j properties for entity of type {entityType}",
            "relationship": f"ValueError: failed to create node of type {entityType}",
            "keyTypes": f"KeyError: failed to find {entityType} in neo4j parameter keys",
            "fromNodeType": "ValueError: from_node_type must be defined.",
            "nodeType": f"TypeError: nodeType must be an instance of class NodeType where the following values are accepted: Person, Place or Thing, not {entityType}.",
            "nodePropsType": "TypeError: from_node_props must be None or a dictionary.",
            "relationshipType": "TypeError: Both from_node_type and to_node_type must be defined to create a relationship.",
        }
        
        self.logger.error(errorMessages[errorType])
        raise Exception(errorMessages[errorType])
        
    def createDyad(self, fromNodeType: str, fromNodeProps: Dict, relationshipType: str, relationshipProps: Dict, toNodeType: str, toNodeProps: Dict) -> str:
        """
        Creates a Cypher dyad (a relationship between two nodes) to be used in a Cypher query.

        Parameters
        ----------
        fromNodeType : str
            The type of the node at the start of the relationship.
        fromNodeProps : dict
            A dictionary containing the properties to be set on the node at the start of the relationship.
        relationshipType : str
            The type of relationship to create.
        relationshipProps : dict
            A dictionary containing the properties to be set on the relationship.
        toNodeType : str
            The type of the node at the end of the relationship.
        toNodeProps : dict
            A dictionary containing the properties to be set on the node at the end of the relationship.

        Returns
        -------
        dyad_str : str
            A string containing the Cypher dyad.
        """
        fromNode = None
        toNode = None
        if fromNodeType:
                fromNode = self.createNode(fromNodeType, fromNodeProps)
                toNode = self.createNode(toNodeType, toNodeProps)
        else:
            self.createDyadErrorHandler(errorType='fromNodeType')


        if fromNode and 'name' not in fromNode:
            self.createDyadErrorHandler(errorType='noNameProp',)
        if toNode and 'name' not in toNode:
            self.createDyadErrorHandler(errorType='noNameProp')

        relationship = self.createRelationship(relationshipType, relationshipProps) if fromNode and toNode else ""      
        return f"{fromNode}{relationship}{toNode}"

    @contextmanager
    def transaction(self, session) -> ResultSummary:
        """
        Creates a context manager to handle Neo4j transactions.

        Parameters
        ----------
        session : neo4j.Session
            A Neo4j session object.

        Yields
        ------
        tx : neo4j.Transaction
            A Neo4j transaction object.
        """ 
        tx = session.begin_transaction()
        try:
            yield tx
            tx.commit()
        except Exception as e:
            self.logger.warn(f"Couldn't insert data due to {e}")
            tx.rollback()
            raise

    def dataPush(self, queriesParams: List[Dict[str, Union[str, Dict]]]) -> bool:
        """
        Connects to the Neo4j database and runs multiple small queries.

        Parameters
        ----------
        queriesParams : list
            A list of dictionaries containing data to be inserted into Neo4j.

        Returns
        -------
        success : bool
            A boolean indicating whether the data insertion was successful.
        """
        queries=[]
        try:
            with self.driver.session() as session:
                with self.transaction(session) as tx:
                    for idx, queryParams in enumerate(queriesParams):
                        queries.append(self.createDyad(queryParams))
                        if idx % self.neo4jParameters['chunk'] == 0:
                            tx.run(f"MERGE {','.join(queries)}", params={})
                            queries = []
                    self.logger.info('neo4j queries have been all written successfully')
                    return True
        except Exception as e:
            self.logger.warn(f"Couldn't insert data due to {e}")
            return False
        
    def close(self):
        self.driver.close()