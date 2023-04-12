from logging import Logger
from neo4j import GraphDatabase
from nodeType import NodeType

class Neo4jHandler(object):
    def __init__(self, valid_node_types, host, user, password, logger: Logger):
        self.valid_node_types = [NodeType(valid_node_type).schema() for valid_node_type in valid_node_types]
        self.driver = GraphDatabase.driver(host=host, auth=(user, password))
        self.logger = logger

    def create_node(self, node_type, node_name):
        if node_type.capitalize() not in self.valid_node_types:
            logger.error(f"Invalid node type detected. Must be one of {', '.join(self.valid_node_types)}")

        return f"(:{node_type.capitalize()} {{name: '{node_name}'}})"

    def create_relationship(self, from_node_type, from_node_name, relationship_type, to_node_type, to_node_name, amount=None):
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
            logger.warn(f"Couldn't parse text due to {e}")
            pass
