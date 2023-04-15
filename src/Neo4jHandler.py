from logging import Logger
from neo4j import GraphDatabase
from contextlib import contextmanager
from neo4j import ResultSummary
from typing import List, Dict, Union

class Neo4jHandler(object):
    def __init__(self, neo4jParameters: Dict, host: str, user: str, password: str, logger: Logger) -> None:
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
        self.parameters = neo4jParameters
        self.driver = GraphDatabase.driver(host=host, auth=(user, password))
        self.logger = logger

    def format_props(self, props: Dict) -> str:
        """
        Formats node or relationship properties as a string to be used in a Cypher query.

        Parameters
        ----------
        props : dict
            A dictionary containing the node or relationship properties.

        Returns
        -------
        prop_str : str
            A string containing the formatted node or relationship properties.
        """
        if props is not None:
            prop_str = ', '.join([f"{k}: '{v}'" for k, v in props.items()])
            return f"{{{prop_str}}}"
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
        rel_props_str = self.format_props(relationshipProps)
        return f"-[:{relationshipType} {{{rel_props_str}}}]->"
        
    def createNode(self, node_type: str, node_props: Dict) -> str:
        """
        Creates a Cypher node string to be used in a Cypher query.

        Parameters
        ----------
        node_type : str
            The type of node to create.
        node_props : dict
            A dictionary containing the properties to be set on the node.

        Returns
        -------
        node_str : str
            A string containing the Cypher node.
        """
        if node_type not in self.nodeTypes:
            self.logger.error(f"Invalid node type detected. Must be one of {', '.join(self.parameters['nodeTypes'])}")
        prop_str = self.format_props(node_props)
        return f"(:{node_type} {{{prop_str}}})"

    def createDyad(self, from_node_type: str, from_node_props: Dict, relationship_type: str, relationship_props: Dict, to_node_type: str, to_node_props: Dict) -> str:
        """
        Creates a Cypher dyad (a relationship between two nodes) to be used in a Cypher query.

        Parameters
        ----------
        from_node_type : str
            The type of the node at the start of the relationship.
        from_node_props : dict
            A dictionary containing the properties to be set on the node at the start of the relationship.
        relationship_type : str
            The type of relationship to create.
        relationship_props : dict
            A dictionary containing the properties to be set on the relationship.
        to_node_type : str
            The type of the node at the end of the relationship.
        to_node_props : dict
            A dictionary containing the properties to be set on the node at the end of the relationship.

        Returns
        -------
        dyad_str : str
            A string containing the Cypher dyad.
        """
        from_node = self.create_node(from_node_type, from_node_props)
        to_node = self.create_node(to_node_type, to_node_props)
        relationship = self.create_relationship(relationship_type, relationship_props)
        return f"{from_node}{relationship}{to_node}"

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
