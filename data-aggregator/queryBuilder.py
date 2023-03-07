import joblib
import os
import sys
import logging
import datetime

sys.path.append('./package')

from package.search import Search
from package.constraintType import ConstraintType
from package.nodeConstraint import NodeConstraint
from package.nodeType import NodeType

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

class QueryBuilder():
    def __init__(self):
        if not os.path.exists(os.path.join(os.getcwd(), 'taxonomies')):
            logger.error('No taxonomies found.')
    
    def build_advanced_query(self, taxonomy_id, filters=None):
        taxonomy = joblib.load(f"taxonomies/taxonomy_{taxonomy_id}.joblib")
        taxonomy_root_node = taxonomy.getStartNode()
        
        constraints = filters.split("AND")[1:] # TODO get entity type instead of [1:]
        
        ncs = []
        for constraint in constraints:
            key, value = constraint.split(':')
            key = key.strip()
            value = value.strip()
            if key == 'email':
                # TODO this should come in the search payload
                EMAIL_ID = 'n68e4e7ae6b9a475198a5c21ef4149098'
                
                nc =NodeConstraint(affectedNodeId=EMAIL_ID,
                    nodeType= NodeType.EMAIL.value,
                    constrainedAttributeName='name',
                    constraintType=ConstraintType.EQUALS,
                    constraintValue=value)
            else:
                nc =NodeConstraint(affectedNodeId=taxonomy_root_node.getId(),
                    nodeType= taxonomy_root_node.getAttribute("NodeType"),
                    constrainedAttributeName=key,
                    constraintType=ConstraintType.STARTSWITH,
                    constraintValue=value)
            ncs.append(nc)
        
        # for filter in filters:
        #     if filter['key'] == 'email':
        #         # TODO this should come in the search payload
        #         EMAIL_ID = 'n68e4e7ae6b9a475198a5c21ef4149098'
                
        #         nc =NodeConstraint(affectedNodeId=EMAIL_ID,
        #             nodeType= NodeType.EMAIL.value,
        #             constrainedAttributeName=filter['key'],
        #             constraintType=ConstraintType.STARTSWITH,
        #             constraintValue=filter['value'])
        #     else:
        #         nc =NodeConstraint(affectedNodeId=taxonomy_root_node.getId(),
        #             nodeType= taxonomy_root_node.getAttribute("NodeType"),
        #             constrainedAttributeName=filter['key'],
        #             constraintType=ConstraintType.STARTSWITH,
        #             constraintValue=filter['value'])
        #     ncs.append(nc)
        s = Search(taxonomy_id,ncs,[])
        return s, taxonomy_root_node
    
    def build_query(self, taxonomy_id,search_term='T'):
        taxonomy = joblib.load(f"taxonomies/taxonomy_{taxonomy_id}.joblib")
        rootNode = taxonomy.getStartNode()
        nc = NodeConstraint(affectedNodeId=rootNode.getId(),
                    nodeType= rootNode.getAttribute("NodeType"),
                    constrainedAttributeName="name",
                    constraintType=ConstraintType.STARTSWITH,
                    constraintValue=search_term)

        query = Search(taxonomy_id,[nc],[])
        return query, rootNode