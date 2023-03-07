import os
import logging
import datetime
import sys
sys.path.append('./package')

from util.neo4jConnection import Neo4jConnection, driver
from package.search import Search
from package.dataSources import DataSources

LOG_USE_STREAM = os.environ.get("LOG_USE_STREAM", True)
LOG_PATH = os.environ.get("LOG_PATH", '/var/log/psi/checkmate')
LOGLEVEL = os.environ.get('LOGLEVEL', 'INFO').upper()

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


NEO4J_URI = os.getenv("NEO4J_URI", 'bolt://neo4j:7687')
NEO4J_CREDENTIALS = os.getenv("NEO4J_AUTH", 'neo4j/test')
USER = NEO4J_CREDENTIALS.split('/')[0]
PASSWORD = NEO4J_CREDENTIALS.split('/')[1]

class QueryExecutor():
    def __init__(self):
        # TODO Define governance policy
        logging.warn('No data policy/governance inplace.')
        self.connection = Neo4jConnection()
        try:
            with driver.session() as session:
                _ = session.run("Match () Return 1 Limit 1", {})
            logger.info('Connection established with Neo4J.')
        except Exception:
            logger.error('Failed to connect with NEO4J.') 
    
    def execute_query(self, query:Search, rootNode, search_term = None):
        # TODO Apply governance policy
        
        try:
            for nc in query.getNodeConstraints():
                if rootNode.getId() == nc.getAffectedNodeId():
                    search_term = nc.constraintValue
            
            expansionQueries, cypherQuery=query.knowledgeGraphDiscoveryQuery([DataSources.CVE.value,DataSources.PEOPLEDATALABS.value, DataSources.DATASCRAPER.value])
        except Exception as err:
            logger.exception(f'Unable to build query:{err}')
        #results = self.connection.query(cypherQuery)
        if not search_term:
            results = self.connection.query("Match (N:Person) Optional Match (N:Person)-[R]-(P) Return * Limit 50")
        else:
            # TODO fix Cypher injection later
            results = self.connection.query(f"Match (N:Person) Where tolower(N.name) =~ tolower('{search_term}') or tolower(N.text) =~ tolower('{search_term}') CALL apoc.path.subgraphAll(N, {{}}) YIELD nodes, relationships RETURN nodes, relationships")
        
        return results