from constraint import Constraint
from nodeType import NodeType
from constraintType import ConstraintType
from node import Node

class NodeConstraint(Constraint):
  def __init__(self,
               affectedNodeId:str,
               nodeType: NodeType,
               constrainedAttributeName:str,
               constraintType:ConstraintType,
               constraintValue):
    Constraint.__init__(self,constrainedAttributeName,constraintType, constraintValue)
    self.nodeType=nodeType
    self.affectedNodeId=affectedNodeId
    # Validate that the constraint type is of the valid name and type for this node type
    if not constrainedAttributeName in Node(NodeType(nodeType)).getAttributesList():
      raise TypeError(f"Attribute {constrainedAttributeName} does not exist for the nodeType {nodeType}.")
    # TODO: See rangeIncludes in the schema, and the definitions here: https://partsol.atlassian.net/wiki/spaces/CHKM/pages/6226060/Query+Builder
    pass

  def getNodeType(self):
    return self.nodeType

  def getAffectedNodeId(self):
    return self.affectedNodeId

  def getAttributesList(self):
    return {"nodeType":self.getNodeType(),
            "affectedNodeId":self.getAffectedNodeId(),
            "constrainedAttributeName":self.getConstrainedAttributeName(),
            "constraintType":self.getConstraintType(),
            "constraintValue":self.constraintValue()
            }
