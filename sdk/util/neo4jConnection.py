import logging
import os

from neo4j import GraphDatabase
from neo4j.exceptions import ClientError
import datetime

NEO4J_URI = os.getenv("NEO4J_URI", 'bolt://neo4j:7687')
NEO4J_CREDENTIALS = os.getenv("NEO4J_AUTH", 'neo4j/test')
USER = NEO4J_CREDENTIALS.split('/')[0]
PASSWORD = NEO4J_CREDENTIALS.split('/')[1]

LOG_USE_STREAM = True
LOG_PATH = os.environ.get("LOG_PATH", '/var/log/psi/checkmate')

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
if LOG_USE_STREAM:
    handler = logging.StreamHandler()
else:
    now = datetime.datetime.now()
    handler = logging.FileHandler(
                LOG_PATH 
                + now.strftime("%Y-%m-%d") 
                + '.log')
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

class Neo4jConnection:
    
    def __init__(self, uri=NEO4J_URI, user=USER, pwd=PASSWORD):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        self.__driver = None
        try:
            self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pwd))
            self.__driver.verify_connectivity()
            logger.info("Connected to NEO4j")
        except Exception as e:
            logger.exception("Failed to create the driver:", e)
        
    def close(self):
        if self.__driver is not None:
            self.__driver.close()
        
    def query(self, query, db=None):
        assert self.__driver is not None, "Driver not initialized!"
        session = None
        response = None
        try: 
            session = self.__driver.session(database=db) if db is not None else self.__driver.session() 
            response = session.run(query).data()
        except Exception as e:
            logger.exception("Query failed:", e)
        finally: 
            if session is not None:
                session.close()
        return response
    
    def query_params(self, query, params, db=None):
        assert self.__driver is not None, "Driver not initialized!"
        session = None
        response = None
        try: 
            session = self.__driver.session(database=db) if db is not None else self.__driver.session() 
            result = session.run(query, {'data': params} )
            response = result.data()
            result.consume()
        except ClientError as err:
            logger.exception("Query failed:", err)
        except Exception as e:
            logger.exception("Query failed:", e)
        finally: 
            if session is not None:
                session.close()
        return response
    
    def __run_cypher(self, transaction, cypher, params):
        result = transaction.run(cypher,params)
        values = [record.values() for record in result]
        result.consume() 
        del result
        return values
    
    def read(self, cypher, params={}):
        result = None
        try:
            with self.__driver.session() as session:
                try:
                    result = session.read_transaction(self.__run_cypher, cypher, params)
                except ClientError as err:
                    logger.exception("Query failed:", err)
                except Exception as e:
                    logger.exception("Query failed:", e)
        except Exception as e:
            logger.exception("Failed to create NEO4J session", e)
    
        return result
    
    def write(self, cypher, params={}):
        result = None
        try:
            with self.driver.session() as session:
                try:
                    result = session.write_transaction(self.__run_cypher, cypher, params)
                except ClientError as err:
                    logger.exception("Query failed:", err)
                except Exception as e:
                    logger.exception("Query failed:", e)
        except Exception as e:
            logger.exception("Failed to create NEO4J session", e)
    
        return result
