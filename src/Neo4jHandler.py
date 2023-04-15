from logging import Logger
from neo4j import GraphDatabase
from nodeType import NodeType
from contextlib import contextmanager
from neo4j import ResultSummary

class Neo4jHandler(object):
    def __init__(self, neo4jParameters, host, user, password, logger: Logger):
        self.parameters = neo4jParameters
        self.driver = GraphDatabase.driver(host=host, auth=(user, password))
        self.logger = logger

    def format_props(self, props):
        if props is not None:
            prop_str = ', '.join([f"{k}: {v}" for k, v in props.items()])
            return f"{{{prop_str}}}"
        else:
            return ""
        
    def createRelationship(self, relationshipType, relationshipProps):
        rel_props_str = self.format_props(relationshipProps)
        return f"-[:{relationshipType} {{{rel_props_str}}}]->"
        
    def createNode(self, node_type, node_props):
        if node_type not in self.nodeTypes:
            self.logger.error(f"Invalid node type detected. Must be one of {', '.join(self.parameters['nodeTypes'])}")
        prop_str = self.format_props(node_props)
        return f"(:{node_type} {{{prop_str}}})"

    def createDyad(self, from_node_type, from_node_props, relationship_type, relationship_props, to_node_type, to_node_props):
        from_node = self.create_node(from_node_type, from_node_props)
        to_node = self.create_node(to_node_type, to_node_props)
        relationship = self.create_relationship(relationship_type, relationship_props)
        return f"{from_node}{relationship}{to_node}"

    @contextmanager
    def transaction(self, session):
        tx = session.begin_transaction()
        try:
            yield tx
            tx.commit()
        except Exception as e:
            self.logger.warn(f"Couldn't insert data due to {e}")
            tx.rollback()
            raise

    def dataPush(self, queriesParams):
        """
        This method connects to the Neo4j database and runs multiple small queries
            
        Parameters
        ----------
        data_generator : generator
            A generator that yields dictionaries of data to be inserted into Neo4j
                
        Return 
        ------
        result
            Return response from Neo4j
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
                    return True
        except Exception as e:
            self.logger.warn(f"Couldn't insert data due to {e}")
            return False
